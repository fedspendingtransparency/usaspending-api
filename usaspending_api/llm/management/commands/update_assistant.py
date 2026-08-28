import json
import logging
from argparse import ArgumentParser

from django.core.management.base import BaseCommand, CommandError
from django.db import models
from pydantic import ValidationError

from usaspending_api.llm.models.db_models import AIModel, Assistant, Prompts
from usaspending_api.llm.models.py_models import InferenceConfig

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Update the configurations of the AI Assistant being used in natural language search."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--name",
            type=str,
            help="Name of the assistant to update (e.g., 'filter-search')",
        )
        parser.add_argument(
            "--model-id",
            type=str,
            help=(
                "Model ID to use (e.g., amazon.nova-lite-v1:0); if used with --model-name, will only evaluate model ID"
            ),
        )
        parser.add_argument(
            "--model-name",
            type=str,
            help='Model name to use (e.g., "claude 4.5"); ignored if used with --model-id',
        )
        parser.add_argument(
            "--clear-system-prompt",
            action="store_true",
            help="Clears the system prompt stored in this Assistant's current instance",
        )
        parser.add_argument(
            "--system-prompt-id",
            type=int,
            help="Primary key of an existing system prompt to use (will swap with the current prompt unless combined)",
        )
        parser.add_argument(
            "--new-system-prompt",
            type=str,
            help="A new system prompt to use in place of existing prompt (will create a new Prompt)",
        )
        parser.add_argument(
            "--combine-prompts",
            action="store_true",
            help=(
                "Concatenates system prompts instead of swapping them (requires --system-prompt-id and/or"
                " --new-system-prompt). Will create a new Prompt with all specified prompts combined"
            ),
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
            help="Comma-separated list of stop sequences (e.g., 'Human:,User:,\\n\\n')",
        )
        parser.add_argument(
            "--inference-config-json",
            type=str,
            help=(
                "Full inference config as JSON string "
                "(e.g., '{\"temperature\": 0.5, \"topP\": 0.8, \"maxTokens\": 5000, \"stopSequences\": []}')"
            ),
        )
        parser.add_argument(
            "--list",
            action="store_true",
            help="List all AI Assistants and their current configs (truncates system prompt for readability)",
        )
        parser.add_argument(
            "--list-with-prompts",
            action="store_true",
            help="List all AI Assistants with their current configs and full system prompt text",
        )
        parser.add_argument(
            "--clear-inference-config",
            action="store_true",
            help="Clear inference config (set to empty dict)",
        )

    def handle(self, *args, **options) -> None:
        # List all AI Assistants.
        if options["list"]:
            self._list_assistants(prompts=options.get("list_with_prompts", False))
            return

        # Retrieve AI Assistant to update.
        assistant = self._get_assistant(options)

        # Check if any update options were provided.
        has_updates = (
            options.get("clear_system_prompt")
            or options.get("model_id")
            or options.get("model_name")
            or options.get("system_prompt_id")
            or options.get("new_system_prompt")
            or options.get("combine_prompts")
            or options.get("temperature") is not None
            or options.get("max_tokens") is not None
            or options.get("top_p") is not None
            or options.get("stop_sequences") is not None
            or options.get("inference_config_json")
            or options.get("clear_inference_config")
        )
        if not has_updates:
            raise CommandError(
                "No update options provided. Use --help to see available options, or --list to view assistants."
            )

        # Clear all current prompts if specified.
        if options["clear_system_prompt"]:
            assistant.system_prompts = None
            assistant.save()

        # Update AI Assistant configurations:
        # 1. Update AI Model to use (only if model options provided).
        if options.get("model_id") or options.get("model_name"):
            model_pk = self._get_model_pk(options)
            assistant.model = model_pk
        # 2. Update system prompt (only if prompt options provided).
        if options.get("system_prompt_id") or options.get("new_system_prompt") or options.get("combine_prompts"):
            prompt_pk = self._get_prompt_pk(assistant, options)
            assistant.system_prompt = prompt_pk
        # 3. Update inference configs (only if inference options provided).
        if (
            options.get("temperature") is not None
            or options.get("max_tokens") is not None
            or options.get("top_p") is not None
            or options.get("stop_sequences") is not None
            or options.get("inference_config_json")
            or options.get("clear_inference_config")
        ):
            inference_configs = self._update_inference_configs(assistant, options)
            assistant.inference_config = inference_configs
        assistant.save()

    def _list_assistants(self, prompts: bool = False) -> None:
        """List all Assistants and their configs."""
        assistants = Assistant.objects.all()

        if not assistants:
            logger.warning("No AI Assistants found.")
            return

        logger.info("\nAI Assistants and Configs:\n")

        for assistant in assistants:
            if prompts:
                logger.info(f"""
                    Assistant: {assistant.name} |
                    Model: {assistant.ai_model.name if assistant.ai_model else 'No Model Selected'} |
                    System Prompt: {assistant.system_prompt.text if assistant.system_prompt else 'None'} |
                    Config: {assistant.inference_config}
                """)
            else:
                logger.info(f"{assistant.__str__()}\n")

    def _get_assistant(self, options: dict) -> Assistant:
        """Retrieve AI Assistant by name."""
        name = options.get("name")
        if not name:
            raise CommandError("Must specify an AI Assistant to retrieve with --name")
        try:
            return Assistant.objects.get(name=name)
        except Assistant.DoesNotExist:
            raise CommandError(f"AI Assistant '{name}' not found.") from None

    def _get_model_pk(self, options: dict) -> int:
        """Retrieve model by ID or name."""
        model_id = options.get("model_id")
        model_name = options.get("model_name")

        # Gives preference to model_id.
        try:
            if model_id:
                model = AIModel.objects.get(model_id=model_id)
                return model.pk
            else:
                model = AIModel.objects.get(name=model_name)
                return model.pk
        except AIModel.DoesNotExist:
            raise CommandError(f"Model not found: {model_id or model_name}.") from None

    def _get_prompt_pk(self, assistant: Assistant, options: dict) -> int:
        """
        Retrieve or create system prompts via command input. This command can either
        swap the current prompt in use with the one specified by `--system-prompt-id`, create
        a new system prompt with `--new-system-prompt` and swap to it, or combine the current prompt in use with a
        new (via `--new-system-prompt`) and/or existing (via `--system-prompt-id`) system prompt
        by using the `--combine-prompts` flag with the other flags.

        Args:
            assistant: The AI Assistant instance being updated.
            options: Command-line options containing prompt information.

        Returns:
            The primary key of the system prompt to use.
        """
        # The current Assistant's system prompt primary key (can be None).
        current_prompt_pk = assistant.system_prompt
        return_prompt_pk = current_prompt_pk
        if not current_prompt_pk:
            # Need current_prompt_pk to be an int for later comparison.
            current_prompt_pk = 0
            return_prompt_pk = None
        # The system prompt specified by --system-prompt-id.
        existing_prompt_pk = options.get("system_prompt_id")
        if existing_prompt_pk and existing_prompt_pk < 1:
            raise CommandError(f"System prompt ID must be an integer greater than 0, got {existing_prompt_pk}")
        # New system prompt to create (string input).
        new_prompt = options.get("new_system_prompt")

        # If prompts are NOT being combined:
        if not options.get("combine_prompts"):
            # If --new-system-prompt and --system-prompt-id provided without --combine-prompts flag, raise an error.
            if new_prompt and existing_prompt_pk:
                raise CommandError("Cannot use --new-system-prompt and --system-prompt-id without --combine-prompts")
            # Keep the same prompt if specified prompt is what's in use and prompts are not being combined.
            elif (existing_prompt_pk == current_prompt_pk):
                logger.info("Specified prompt is the same as the one currently in use. No changes made to prompt.")
            # If --system-prompt-id is provided and --combine-prompts is not, return existing_prompt_pk.
            elif existing_prompt_pk:
                return_prompt_pk = existing_prompt_pk
            # If --new-system-prompt is provided and --combine-prompts is not, create a new prompt and return its pk.
            elif new_prompt:
                return_prompt_pk = self._create_new_prompt([new_prompt])
        # If prompts ARE being combined:
        else:
            # Get the text of the prompt currently in use.
            current_prompt = Prompts.objects.get(pk=current_prompt_pk)
            current_prompt_text = current_prompt.text if current_prompt else ""
            # Get the text of the new prompt.
            new_prompt_text = new_prompt if new_prompt else ""
            # Combine the prompts.
            if existing_prompt_pk:
                existing_prompt = Prompts.objects.get(pk=existing_prompt_pk)
                existing_prompt_text = existing_prompt.text if existing_prompt else ""
                combined_prompt_text = [current_prompt_text, existing_prompt_text, new_prompt_text]
            else:
                combined_prompt_text = [current_prompt_text, new_prompt_text]
            # Create a new prompt with the combined text.
            return_prompt_pk = self._create_new_prompt(combined_prompt_text)
        return return_prompt_pk

    def _create_new_prompt(self, text: list) -> int:
        """
        Create a new system prompt with the given list of text.

        Args:
            text: List of strings to be joined with newlines.

        Returns:
            int: The primary key of the newly created prompt.
        """
        # Iterate the pk on the Prompts table to generate a unique name.
        next_pk = (Prompts.objects.aggregate(max_pk=models.Max('pk'))['max_pk'] or 0) + 1
        # Craft the new Prompt to be created in the DB.
        Prompts.objects.create(
            name=f"Custom Prompt #{next_pk}",
            pk=next_pk,
            description="Custom prompt created by 'update_assistant' management command",
            text="\n".join(text)
        )
        return next_pk

    def _update_inference_configs(self, assistant: Assistant, options: dict) -> json:
        """Takes the command input and produces a new dictionary of inference configs."""
        # Handle --clear-inference-config flag.
        if options.get("clear_inference_config"):
            return {}
        # Get the current config object.
        current_config = assistant.inference_config
        new_config = current_config
        # Handle edge case of individual configs and full configs getting provided together in one command:
        if options.get("inference_config_json") and (
            options.get("temperature")
            or options.get("max_tokens")
            or options.get("top_p")
            or options.get("stop_sequences")
        ):
            raise CommandError(
                "Cannot provide both individual inference config options and a full inference config JSON string."
            )
        # If config options are being changed individually and not as a JSON string:
        if not options.get("inference_config_json"):
            # Handle each field of the inference config separately.
            temperature = options.get("temperature")
            max_tokens = options.get("max_tokens")
            top_p = options.get("top_p")
            stop_sequences = options.get("stop_sequences")
            new_config = {
                "temperature": temperature if temperature is not None else current_config.get("temperature"),
                "maxTokens": max_tokens if max_tokens is not None else current_config.get("maxTokens"),
                "topP": top_p if top_p is not None else current_config.get("topP"),
                "stopSequences": stop_sequences if stop_sequences is not None else current_config.get("stopSequences")
            }
            # Validate the new config using Pydantic model.
            try:
                InferenceConfig(**new_config)
            except ValidationError as e:
                raise CommandError(f"Invalid inference config: {e}") from e
        # If a JSON string is provided for the entire config dict:
        else:
            new_config_json = options.get("inference_config_json")
            # Validate the JSON config using Pydantic model.
            try:
                config_dict = json.loads(new_config_json)
                # Merge with current config to allow partial updates from inference_config_json flag.
                # This allows the use of --inference-config-json without specifying the entire set of config options.
                # Example: --inference-config-json '{"temperature": 0.7, "maxTokens": 1000}'
                # ^^ This only updates those 2 values, the others remain unchanged from the current config.
                # To let the AI model use its own defaults, set values to null:
                # Example: --inference-config-json '{"maxTokens": null, "stopSequences": null}'
                # ^^ This removes those parameters from the inference request, allowing the model to use its defaults.
                merged_config = {**current_config, **config_dict}
                InferenceConfig(**merged_config)
                new_config = merged_config
            except ValidationError as e:
                raise CommandError(f"Invalid inference config: {e}") from e
            except json.JSONDecodeError as e:
                raise CommandError(f"Invalid JSON in inference config: {e}") from e
        return new_config
