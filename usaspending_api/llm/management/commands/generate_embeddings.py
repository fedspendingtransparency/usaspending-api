import logging
from argparse import ArgumentParser
from typing import Type

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Model, QuerySet

from usaspending_api.common.mixins import EmbeddingMixin

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "model", type=str, help='Model name (e.g., "naics", "psc", "cfda", "toptieragency") or "all"'
        )
        parser.add_argument("--app", type=str, default="references", help="Django app name (default: references)")
        parser.add_argument(
            "--batch-size", type=int, default=50, help="Number of records to process in each batch (default: 50)"
        )
        parser.add_argument("--force", action="store_true", help="Regenerate embeddings even if they already exist")
        parser.add_argument(
            "--dry-run", action="store_true", help="Show what would be processed without making changes"
        )
        parser.add_argument("--limit", type=int, help="Limit number of records to process (useful for testing)")
        parser.add_argument("--verbose", action="store_true", help="Show detailed progress for each record")

    def handle(self, *args, **options) -> None:
        model_name = options["model"].lower()
        app_name = options["app"]
        batch_size = options["batch_size"]
        force = options["force"]
        dry_run = options["dry_run"]
        limit = options.get("limit")
        verbose = options["verbose"]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be made"))

        # Get models to process
        if model_name == "all":
            models_to_process = self.get_all_embedding_models()
            if not models_to_process:
                self.stdout.write(self.style.ERROR("No models found with EmbeddingMixin"))
                return
            self.stdout.write(f"Found {len(models_to_process)} models with EmbeddingMixin:")
            for model in models_to_process:
                self.stdout.write(f"  - {model.__name__}")
        else:
            try:
                model_class = apps.get_model(app_name, model_name)
                if not issubclass(model_class, EmbeddingMixin):
                    raise CommandError(
                        f"{model_name} does not use EmbeddingMixin. "
                        f"Add EmbeddingMixin to the model to use this command."
                    )
                models_to_process = [model_class]
            except LookupError:
                raise CommandError(
                    f"Model '{model_name}' not found in app '{app_name}'. Use --app to specify a different app."
                ) from None

        # Process each model
        total_processed = 0
        total_failed = 0

        for model_class in models_to_process:
            processed, failed = self.process_model(
                model_class=model_class,
                batch_size=batch_size,
                force=force,
                dry_run=dry_run,
                limit=limit,
                verbose=verbose,
            )
            total_processed += processed
            total_failed += failed

        # Summary
        if dry_run:
            self.stdout.write(self.style.WARNING(f"DRY RUN COMPLETE: Would process {total_processed} records"))
        else:
            self.stdout.write(
                self.style.SUCCESS(f"COMPLETE: Processed {total_processed} records, {total_failed} failed")
            )

    @staticmethod
    def get_all_embedding_models() -> list[Model]:
        """Find all models that use EmbeddingMixin"""
        embedding_models = []
        for model in apps.get_models():
            if issubclass(model, EmbeddingMixin) and not model._meta.abstract:
                embedding_models.append(model)
        return embedding_models

    def process_model(
        self,
        model_class: Type[Model],
        batch_size: int,
        force: bool,
        dry_run: bool,
        limit: int = None,
        verbose: bool = False,
    ) -> tuple[int, int]:
        """Process a single model"""
        model_name = model_class.__name__
        dimensions = getattr(model_class, "embedding_dimensions", 256)
        self.stdout.write(f"Processing: {model_name} (dimensions: {dimensions})")

        queryset = self._build_queryset(model_class, force, limit)
        total = queryset.count()

        if total == 0:
            self.stdout.write(self.style.SUCCESS(f"No {model_name} records need embeddings"))
            return 0, 0

        pks_to_process = list(queryset.values_list("pk", flat=True))
        self.stdout.write(f"Found {len(pks_to_process):,} records to process")

        if dry_run:
            self.stdout.write(self.style.WARNING(f"Would process {len(pks_to_process):,} {model_name} records"))
            return len(pks_to_process), 0

        # Process in batches
        processed = 0
        failed = 0
        skipped = 0

        for i in range(0, len(pks_to_process), batch_size):
            batch_pks = pks_to_process[i : i + batch_size]
            batch = list(model_class.objects.filter(pk__in=batch_pks))
            batch_num = (i // batch_size) + 1
            total_batches = (len(pks_to_process) + batch_size - 1) // batch_size

            self.stdout.write(f"\nBatch {batch_num}/{total_batches} ({len(batch)} records)")

            for instance in batch:
                try:
                    num_processed, num_skipped = self._process_instance(instance, force, verbose)
                    processed += num_processed
                    skipped += num_skipped
                except Exception as e:
                    failed += 1
                    identifier = f"{instance.__class__.__name__}(pk={instance.pk})"
                    self.stdout.write(self.style.ERROR(f"  Failed {identifier}: {str(e)}"))
                    pk_value = getattr(instance, "pk", "unknown")
                    logger.exception(f"Failed to generate embedding for {model_name} {pk_value}")

            # Progress update
            progress_pct = ((i + len(batch)) / total) * 100
            self.stdout.write(
                f"Progress: {processed:,} processed, {skipped:,} skipped, {failed:,} failed ({progress_pct:.1f}%)"
            )

        # Model summary
        if failed > 0:
            self.stdout.write(
                self.style.WARNING(f"{model_name}: {processed:,} processed, {skipped:,} skipped, {failed:,} FAILED")
            )
        else:
            self.stdout.write(self.style.SUCCESS(f"{model_name}: {processed:,} processed, {skipped:,} skipped"))

        return processed, failed

    def _build_queryset(self, model_class: Type[Model], force: bool, limit: int) -> QuerySet:
        # Build queryset
        if force:
            queryset = model_class.objects.all()
        else:
            queryset = model_class.objects.filter(embedding__isnull=True)

        # Apply limit if provided
        if limit:
            queryset = queryset[:limit]
            self.stdout.write(f"Limited to {limit} records")
        return queryset

    def _process_instance(self, instance: Model, force: bool, verbose: bool) -> tuple[int, int]:
        identifier = f"{instance.__class__.__name__}(pk={instance.pk})"

        processed = 0
        skipped = 0

        # Check if embedding text is available
        text = instance.get_embedding_text()
        if not text:
            if verbose:
                self.stdout.write(f"  Skipped {identifier}: No text available")
            skipped += 1

        # Generate embedding
        if not instance.has_embedding or force:
            instance.save(
                verbose=verbose,
                force_generate_embedding=force,
                update_fields=["embedding", "embedding_generated_at"],
            )
            processed += 1
        else:
            if verbose:
                self.stdout.write(f"  Skipped {identifier}: Already exists")
            skipped += 1
        return processed, skipped
