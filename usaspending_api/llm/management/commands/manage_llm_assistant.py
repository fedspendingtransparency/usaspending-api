import json
import logging
import uuid

from django.core.management.base import BaseCommand, CommandError, CommandParser
from pydantic import ValidationError

from usaspending_api.llm.models.db_models import AIModel, Assistant, Prompts
from usaspending_api.llm.models.py_models import InferenceConfig

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Update the configurations of the AI Assistant being used in natural language search."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--name",
            type=str,
            help="Name of the assistant to update (e.g., 'filter-search')",
        )
        parser.add_argument(
            "--pk",
            type="int",
            help="Primary key of the assistant to update (e.g., 1)",
        )
        parser.add_argument(
            "--create-new",
            action="store_true",
            help="Create a new AI Assistant with the specified name and configurations (requires name and AI model)",
        )
        parser.add_argument(
            "--model-id",
            type=str,
            help=(
                "Model ID to use (e.g., 'amazon.nova-lite-v1:0'); if used with --model-name, will only use model ID"
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
            help="Clears the system prompt stored in this Assistant's record",
        )
        parser.add_argument(
            "--system-prompt-id",
            type=int,
            help="Primary key of an existing system prompt to use (will swap with the current prompt unless combined)",
        )
        parser.add_argument(
            "--new-system-prompt",
            type=str,
            help="A new system prompt to use in place of existing prompt (will always create a new Prompt)",
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
            "--new-prompt-name",
            type=str,
            help="Provide a name for your system prompt (if not provided, a default one will be generated)"
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
        parser.add_argument(
            "--is-active",
            action="store_true",
            help="Activate this assistant."
        )
        parser.add_argument(
            "--is-inactive",
            action="store_true",
            help="Deactivate this assistant."
        )
        parser.add_argument(
            "--description",
            type=str,
            help="Description of the assistant (optional)"
        )

    def handle(self, *args, **options) -> None:
        # List all AI Assistants.
        if options.get("list") or options.get("list_with_prompts"):
            self._list_assistants(prompts=options.get("list_with_prompts", False))
            return

        # Create new AI Assistant or retrieve AI Assistant to update.
        if options.get("create_new"):
            if not options.get("name"):
                raise CommandError("Must specify a name for the new AI Assistant with --name")
            # Create a new assistant with the specified name and active state (and any configs specified).
            assistant = self._create_assistant(options)
            logger.info(f"Created new AI Assistant '{assistant.name}' (pk: {assistant.pk})")
            return
        else:
            assistant = self._get_assistant(options)

        # Check if any update options were provided.
        has_updates = (
            options.get("clear_system_prompt")
            or options.get("model_id") is not None
            or options.get("model_name") is not None
            or options.get("system_prompt_id") is not None
            or options.get("new_system_prompt") is not None
            or options.get("combine_prompts")
            or options.get("new_prompt_name") is not None
            or options.get("temperature") is not None
            or options.get("max_tokens") is not None
            or options.get("top_p") is not None
            or options.get("stop_sequences") is not None
            or options.get("inference_config_json") is not None
            or options.get("clear_inference_config")
            or options.get("is_active")
            or options.get("is_inactive")
            or options.get("description") is not None
        )
        if not has_updates:
            raise CommandError(
                "No update options provided. Use --help to see available options, or --list to view assistants."
            )

        # Clear all current system prompts if specified.
        if options.get("clear_system_prompt"):
            assistant.system_prompts = None
            assistant.save()

        # Update AI Assistant configurations:
        # 1. Update AI Model to use (if provided).
        if options.get("model_id") or options.get("model_name"):
            model_pk = self._get_model_pk(options)
            assistant.model = model_pk
        # 2. Update system prompt (if prompt options provided).
        if options.get("system_prompt_id") or options.get("new_system_prompt"):
            prompt_pk = self._get_prompt_pk(assistant, options)
            assistant.system_prompt = prompt_pk
        # 3. Update inference configs (if provided).
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
        # 4. Update the assistant's active state (if provided).
        if options.get("is_active") or options.get("is_inactive"):
            active_state = self._update_active_state(options)
            if active_state is not None:
                assistant.is_active = active_state
        # 5. Update the assistant's description (if provided).
        if options.get("description"):
            assistant.description = options.get("description")
        # 6. Save all changes.
        assistant.save()

    def _create_assistant(self, options: dict) -> Assistant:
        """Create a new AI Assistant with the specified name and configurations."""
        if options.get("model_id") or options.get("model_name"):
            model_pk = self._get_model_pk(options)
        else:
            model_pk = None
        # Check if an active assistant with the same name already exists.
        existing_assistant = Assistant.objects.filter(name=options.get("name"), is_active=True).first()
        # If there's already an active assistant with the same name, but the user has indicated --is-active,
        # deactivate the existing assistants and allow the new one to be created and set to active.
        if existing_assistant and options.get("is_active"):
            # Calling _update_active_state() will automatically deactivate all Assistants with the passed in --name.
            active_state = self._update_active_state(options)
        else:
            active_state = False
        assistant = Assistant.objects.create(
            name=options.get("name"),
            ai_model=model_pk,
            system_prompt=None,
            inference_config={},
            is_active=active_state,
            description=options.get("description") if options.get("description") else None,
        )
        has_configs = False
        # Update the system prompt if specified.
        if options.get("system_prompt_id") or options.get("new_system_prompt"):
            has_configs = True
            assistant.system_prompt = self._get_prompt_pk(assistant, options)
        # Update the inference configs if specified.
        if (
                options.get("temperature") is not None
                or options.get("max_tokens") is not None
                or options.get("top_p") is not None
                or options.get("stop_sequences") is not None
                or options.get("inference_config_json") is not None
        ):
            has_configs = True
            inference_configs = self._update_inference_configs(assistant, options)
            assistant.inference_config = inference_configs
        if has_configs:
            assistant.save()
        return assistant

    @staticmethod
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
                    Assistant: {assistant.name} (PK: {assistant.pk}) |
                    Model: {assistant.ai_model.name if assistant.ai_model else 'No Model Selected'} |
                    Active: {assistant.is_active} |
                    System Prompt: {assistant.system_prompt.text if assistant.system_prompt else 'None'} |
                    Config: {assistant.inference_config} |
                    Description: {assistant.description}
                """)
            else:
                logger.info(f"{assistant.__str__()}\n")

    @staticmethod
    def _get_assistant(self, options: dict) -> Assistant:
        """Retrieve AI Assistant."""
        name = options.get("name")
        pk = options.get("pk")
        # Selection logic:
        # 1. If neither pk nor name is provided, raise an error.
        # 2. If pk is provided, retrieve by pk.
        # 3. If name is provided and no pk, retrieve by name where is_active is also true.
        # 4. If name is provided and no pk, but no active assistant found, raise an error.
        if not pk and not name:
            raise CommandError("Must specify an AI Assistant to retrieve with --name or --pk")
        if pk:
            try:
                return Assistant.objects.get(pk=pk)
            except Assistant.DoesNotExist:
                raise CommandError(f"AI Assistant with pk '{pk}' not found.") from None
        if name and not pk:
            try:
                return Assistant.objects.get(name=name, is_active=True)
            except Assistant.DoesNotExist:
                raise CommandError(f"Active AI Assistant with name '{name}' not found.") from None

    @staticmethod
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
        Retrieve or create system prompts via command input. This command can either:
        1. Swap the current prompt in use with the one specified by `--system-prompt-id`;
        2. create a new system prompt with `--new-system-prompt` and swap to it; or,
        3. combine the current prompt in use with a new one (via `--new-system-prompt`) and/or an existing one
        (via `--system-prompt-id`) by using the `--combine-prompts` flag with the other flags.
        NOTE: the `--combine-prompts` flag will always use the current prompt in use to begin combining the
        other prompts created/specified. You can clear the current prompt in use with `--clear-system-prompt` if you
        want to start with a blank prompt and combine only the new and/or existing prompts.

        Args:
            assistant: The AI Assistant instance being updated.
            options: Command-line options containing prompt information.

        Returns:
            The primary key of the system prompt to use.
        """
        # The current Assistant's system prompt primary key (can be None).
        current_prompt_pk = assistant.system_prompt.pk
        # The final prompt primary key to return.
        return_prompt_pk = current_prompt_pk
        if not current_prompt_pk:
            # Need current_prompt_pk to be an int for later comparison.
            current_prompt_pk = 0
            return_prompt_pk = None
        # The system prompt specified by --system-prompt-id.
        existing_prompt_pk = options.get("system_prompt_id")
        if existing_prompt_pk and existing_prompt_pk < 1:
            raise CommandError(f"System prompt ID must be an integer greater than 0, got {existing_prompt_pk}")
        # New system prompt to create as specified by --new-system-prompt.
        new_prompt = options.get("new_system_prompt")
        new_prompt_name = options.get("new_prompt_name")
        # If the system prompts are NOT being combined, the active prompt is swapped with the one specified:
        if not options.get("combine_prompts"):
            return_prompt_pk = self._handle_swap_prompts(
                                    current_prompt_pk,
                                    existing_prompt_pk,
                                    new_prompt,
                                    new_prompt_name
                                )
        # If prompts ARE being combined:
        else:
            return_prompt_pk = self._handle_combine_prompts(current_prompt_pk, existing_prompt_pk, new_prompt, new_prompt_name)
        return return_prompt_pk

    def _handle_swap_prompts(self, current_prompt_pk: int, existing_prompt_pk: int, new_prompt: str, new_prompt_name: str) -> int | None:
        """Handles swapping system prompts as specified by command options."""
        # If --new-system-prompt and --system-prompt-id provided without --combine-prompts flag, raise an error.
        if new_prompt and existing_prompt_pk:
            raise CommandError("Cannot use --new-system-prompt and --system-prompt-id without --combine-prompts")
        # Keep the same prompt if specified prompt is what's in use and prompts are not being combined.
        elif existing_prompt_pk == current_prompt_pk:
            logger.info("Specified prompt is the same as the one currently in use. No changes made to prompt.")
        # If --system-prompt-id is provided and --combine-prompts is not, return existing_prompt_pk.
        elif existing_prompt_pk:
            return_prompt_pk = existing_prompt_pk
        # If --new-system-prompt is provided and --combine-prompts is not, create a new prompt and return its pk.
        elif new_prompt:
            return_prompt_pk = self._create_new_prompt([new_prompt], new_prompt_name)
        return return_prompt_pk

    def _handle_combine_prompts(self, current_prompt_pk: int, existing_prompt_pk: int, new_prompt: str, new_prompt_name: str) -> int:
        """Handles combining system prompts as specified by command options."""
        # Get the text of the prompt currently in use.
        if current_prompt_pk and current_prompt_pk > 0:
            current_prompt = Prompts.objects.get(pk=current_prompt_pk)
            current_prompt_text = current_prompt.text
        else:
            current_prompt_text = ""
        # Get the text of the new prompt being created via flag.
        new_prompt_text = new_prompt if new_prompt else ""
        # Combine the prompts.
        if existing_prompt_pk:
            existing_prompt = Prompts.objects.get(pk=existing_prompt_pk)
            existing_prompt_text = existing_prompt.text if existing_prompt else ""
            combined_prompt_text = [current_prompt_text, existing_prompt_text, new_prompt_text]
        else:
            combined_prompt_text = [current_prompt_text, new_prompt_text]
        # Create a new prompt with the combined text.
        return_prompt_pk = self._create_new_prompt(combined_prompt_text, new_prompt_name)
        return return_prompt_pk

    @staticmethod
    def _create_new_prompt(self, text: list, new_prompt_name: str) -> int:
        """
        Create a new system prompt with the given list of text. Prompt name will be a random UUID if name not specified.

        Args:
            text: List of strings to be joined with newlines.

        Returns:
            int: The primary key of the newly created prompt.
        """
        # Filter out empty strings to avoid extra newlines.
        filtered_text = [t for t in text if t]
        # Craft the new Prompt to be created in the DB.
        prompt = Prompts.objects.create(
            name=new_prompt_name if new_prompt_name else str(uuid.uuid4()),
            description="Custom prompt created by management command: 'manage_llm_assistant'",
            text="\n".join(filtered_text)
        )
        return prompt.pk

    def _update_inference_configs(self, assistant: Assistant, options: dict) -> dict:
        """Takes the command input and produces a new dictionary of inference configs."""
        # Handle --clear-inference-config flag.
        if options.get("clear_inference_config"):
            return {}
        # Get the current config object.
        current_config = assistant.inference_config
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
            new_config = self._handle_individual_inference_configs(options, current_config)
        # If a JSON string is provided for the entire config dict:
        else:
            new_config = self._handle_inference_config_json(options, current_config)
        return new_config if new_config else current_config

    @staticmethod
    def _handle_individual_inference_configs(self, options: dict, current_config: dict) -> dict:
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
        return new_config

    @staticmethod
    def _handle_inference_config_json(self, options: dict, current_config: dict) -> dict:
        new_config_json = options.get("inference_config_json")
        # Validate the JSON config using Pydantic model.
        try:
            config_dict = json.loads(str(new_config_json))
            # Merge with current config to allow partial updates from inference_config_json.
            # This allows the use of --inference-config-json without specifying the entire set of config options.
            # Example: --inference-config-json '{"temperature": 0.7, "maxTokens": 1000}'
            # ^^ This only updates those 2 values, the others remain unchanged from the current config.
            # To let an AI model use its own defaults, set values to null.
            # Example: --inference-config-json '{"maxTokens": null, "stopSequences": null}'
            # ^^ Removes those parameters from the request, allowing the Bedrock LLM to use its own defaults.
            merged_config = {**current_config, **config_dict}
            InferenceConfig(**merged_config)
            new_config = merged_config
        except ValidationError as e:
            raise CommandError(f"Invalid inference config: {e}") from e
        except json.JSONDecodeError as e:
            raise CommandError(f"Invalid JSON in inference config: {e}") from e
        return new_config

    @staticmethod
    def _update_active_state(self, options: dict) -> bool | None:
        """Update the active state of the assistant."""
        is_active = options.get("is_active")
        is_inactive = options.get("is_inactive")

        if is_active and is_inactive:
            raise CommandError("Cannot specify both --is-active and --is-inactive.")

        if is_active:
            # Deactivate any currently active assistants with the same name.
            active_assistants = Assistant.objects.filter(name=options.get("name"), is_active=True)
            if active_assistants:
                for active_assistant in active_assistants:
                    active_assistant.is_active = False
                    active_assistant.save(update_fields=["is_active"])
            return True
        elif is_inactive:
            return False
        else:
            return None
