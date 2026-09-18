import logging
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError, CommandParser

from usaspending_api.llm.evals.exceptions import EvalError
from usaspending_api.llm.evals.ingestion.pipeline import ingest_ground_truth
from usaspending_api.llm.evals.ingestion.providers import HttpFileProvider, LocalFileProvider
from usaspending_api.llm.evals.loader import dataset_directory

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Convert a stakeholder Excel workbook into the runtime JSON dataset."""

    help = "Ingest an Excel ground-truth workbook into ground_truth.json."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--source",
            choices=("local", "http"),
            default="local",
            help="Workbook source provider",
        )
        parser.add_argument(
            "--reference",
            required=True,
            help="Local Excel spreadsheet file path or HTTPS download URL",
        )
        parser.add_argument(
            "--output",
            type=Path,
            help="Output JSON path. Defaults to the configured eval data directory in settings",
        )

    def handle(self, *args, **options) -> None:
        provider = LocalFileProvider() if options["source"] == "local" else HttpFileProvider()
        output_path = options["output"] or dataset_directory() / "ground_truth.json"

        try:
            result = ingest_ground_truth(
                provider=provider,
                source_reference=options["reference"],
                output_path=output_path,
            )
        except EvalError as exc:
            raise CommandError(str(exc)) from exc

        version = f" ({result.source_version})" if result.source_version else ""
        logger.info(
            f"Generated {result.case_count} ground-truth cases and "
            f"loaded {result.mapping_count} mappings from "
            f"{result.source_name}{version} into {result.output_path}."
        )
