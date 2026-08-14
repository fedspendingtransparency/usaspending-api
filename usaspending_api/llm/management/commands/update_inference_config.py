import json
from argparse import ArgumentParser

from django.core.management.base import BaseCommand, CommandError

from usaspending_api.llm.models.db_models import AIModel


class Command(BaseCommand):
    help = "Update inference configurations for AI models used in natural language search."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--model-id",
            type=str,
            help="Model ID to update (e.g., anthropic.claude-4-5-sonnet)",
        )
        parser.add_argument(
            "--model-name",
            type=str,
            help='Model name to update (e.g., "claude 4.5")',
        )
        parser.add_argument(
            "--temperature",
            type=float,
            help="Temperature value (0.0 - 1.0)",
        )
        parser.add_argument(
            "--top-p",
            type=float,
            help="Top P value (0.0 - 1.0)",
        )
        parser.add_argument(
            "--max-tokens",
            type=int,
            help="Maximum tokens to generate",
        )
        parser.add_argument(
            "--stop-sequences",
            type=str,
            help='Comma-separated list of stop sequences (e.g., "Human:,User:,\\n\\n")',
        )
        parser.add_argument(
            "--config-json",
            type=str,
            help=(
                "Full inference config as JSON string "
                '(e.g., \'{"temperature": 0.5, "topP": 0.8, "maxTokens": 2048, "stopSequences": []}\')'
            ),
        )
        parser.add_argument(
            "--list",
            action="store_true",
            help="List all models and their current inference configs",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Clear inference config (set to empty dict)",
        )

    def handle(self, *args, **options) -> None:
        if options["list"]:
            self._list_models()
            return

        model = self._get_model(options)

        if options["clear"]:
            self._clear_config(model)
            return

        if options["config_json"]:
            self._update_config_from_json(model, options["config_json"])
            return

        self._update_config_from_options(model, options)

    def _get_model(self, options: dict) -> AIModel:
        """Retrieve model by ID or name."""
        model_id = options.get("model_id")
        model_name = options.get("model_name")

        if not model_id and not model_name:
            raise CommandError("Must specify either --model-id or --model-name")

        try:
            if model_id:
                return AIModel.objects.get(model_id=model_id)
            else:
                return AIModel.objects.get(name=model_name)
        except AIModel.DoesNotExist:
            raise CommandError(f"Model not found: {model_id or model_name}.") from None

    def _clear_config(self, model: AIModel) -> None:
        """Clear inference config for a model."""
        model.inference_config = {}
        model.save()
        self.stdout.write(self.style.SUCCESS(f"Cleared inference config for {model.name}."))

    def _update_config_from_json(self, model: AIModel, config_json: str) -> None:
        """Update inference config from JSON string."""
        try:
            config = json.loads(config_json)
            # Validate the config values.
            self._validate_config(config)
            model.inference_config = config
            model.save()
            self.stdout.write(
                self.style.SUCCESS(f"Updated inference config for {model.name}:\n{json.dumps(config, indent=2)}")
            )
        except json.JSONDecodeError as e:
            raise CommandError(f"Invalid JSON: {e}") from e

    def _update_config_from_options(self, model: AIModel, options: dict) -> None:
        """Update inference config from individual options."""
        config = model.inference_config or {}

        if options["temperature"] is not None:
            self._validate_temperature(options["temperature"])
            config["temperature"] = options["temperature"]
        if options["top_p"] is not None:
            self._validate_top_p(options["top_p"])
            config["topP"] = options["top_p"]
        if options["max_tokens"] is not None:
            self._validate_max_tokens(options["max_tokens"])
            config["maxTokens"] = options["max_tokens"]
        if options["stop_sequences"] is not None:
            stop_sequences = self._parse_stop_sequences(options["stop_sequences"])
            self._validate_stop_sequences(stop_sequences)
            config["stopSequences"] = stop_sequences

        if not config:
            raise CommandError(
                "Must specify at least one config option: "
                "--temperature, --top-p, --max-tokens, --stop-sequences, or --config-json"
            )

        model.inference_config = config
        model.save()

        self.stdout.write(
            self.style.SUCCESS(f"Updated inference config for {model.name}:\n{json.dumps(config, indent=2)}")
        )

    def _validate_temperature(self, value: float) -> None:
        """Validate temperature is between 0.0 and 1.0."""
        if not 0.0 <= value <= 1.0:
            raise CommandError(f"Invalid temperature: {value}. Must be between 0.0 and 1.0.")

    def _validate_top_p(self, value: float) -> None:
        """Validate topP is between 0.0 and 1.0."""
        if not 0.0 <= value <= 1.0:
            raise CommandError(f"Invalid top-p: {value}. Must be between 0.0 and 1.0.")

    def _validate_max_tokens(self, value: int) -> None:
        """Validate maxTokens is a positive integer."""
        if value <= 0:
            raise CommandError(f"Invalid max-tokens: {value}. Must be a positive integer.")

    def _parse_stop_sequences(self, stop_sequences_str: str) -> list[str]:
        """Parse comma-separated stop sequences string into list."""
        if not stop_sequences_str:
            return []
        # Split by comma and strip whitespace, handle escaped newlines.
        sequences = [seq.strip().replace("\\n", "\n") for seq in stop_sequences_str.split(",")]
        return [seq for seq in sequences if seq]  # Filter out empty strings.

    def _validate_stop_sequences(self, value: list) -> None:
        """Validate stopSequences is a list of strings."""
        if not isinstance(value, list):
            raise CommandError(f"Invalid stop-sequences type: {type(value).__name__}. Must be a list")
        for i, seq in enumerate(value):
            if not isinstance(seq, str):
                raise CommandError(
                    f"Invalid stop sequence at index {i}: {type(seq).__name__}. All stop sequences must be strings."
                )

    def _validate_config(self, config: dict) -> None:
        """Validate all values in config dict."""
        if "temperature" in config:
            if not isinstance(config["temperature"], (int, float)):
                raise CommandError(
                    f"Invalid temperature type: {type(config['temperature']).__name__}. Must be a number."
                )
            self._validate_temperature(config["temperature"])

        if "topP" in config:
            if not isinstance(config["topP"], (int, float)):
                raise CommandError(f"Invalid topP type: {type(config['topP']).__name__}. Must be a number.")
            self._validate_top_p(config["topP"])

        if "maxTokens" in config:
            if not isinstance(config["maxTokens"], int):
                raise CommandError(f"Invalid maxTokens type: {type(config['maxTokens']).__name__}. Must be an integer.")
            self._validate_max_tokens(config["maxTokens"])

        if "stopSequences" in config:
            self._validate_stop_sequences(config["stopSequences"])

    def _list_models(self) -> None:
        """List all models and their inference configs."""
        models = AIModel.objects.all()

        if not models:
            self.stdout.write(self.style.WARNING("No models found."))
            return

        self.stdout.write(self.style.SUCCESS("\nAI Models and Inference Configs:\n"))

        for model in models:
            self.stdout.write(f"\n{self.style.HTTP_INFO(model.name)} ({model.model_id})")
            self.stdout.write(f"  Provider: {model.provider}")

            if model.inference_config:
                self.stdout.write("  Inference Config:")
                for key, value in model.inference_config.items():
                    self.stdout.write(f"    {key}: {value}")
            else:
                self.stdout.write(f"  Inference Config: {self.style.WARNING('(empty - will use defaults)')}")
