import json
import logging
import uuid

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import IntegrityError, transaction
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
            type=int,
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
            action="append",
            help=(
                "Stop sequence to add; repeat the option for multiple sequences, allowing commas within a sequence "
                "(e.g., --stop-sequences 'Human:,User:')"
            ),
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

    @transaction.atomic
    def handle(self, *args, **options) -> None:
        # List all AI Assistants.
        if options.get("list") or options.get("list_with_prompts"):
            self._validate_list_options(options)
            self._list_assistants(prompts=options.get("list_with_prompts", False))
            return

        self._validate_options(options)
        if options.get("create_new"):
            if not options.get("name"):
                raise CommandError("Must specify a name for the new AI Assistant with --name")
            assistant = self._create_assistant(options)
            logger.info(f"Created new AI Assistant '{assistant.name}' (pk: {assistant.pk})")
            return

        assistant = self._get_assistant(options)
        if not self._has_updates(options):
            raise CommandError(
                "No update options provided. Use --help to see available options, or --list to view assistants."
            )
        self._apply_updates(assistant, options)

    @staticmethod
    def _validate_list_options(options: dict) -> None:
        if options.get("list") and options.get("list_with_prompts"):
            raise CommandError("Use either --list or --list-with-prompts, not both.")

        value_options = (
            "name",
            "pk",
            "model_id",
            "model_name",
            "system_prompt_id",
            "new_system_prompt",
            "new_prompt_name",
            "temperature",
            "top_p",
            "max_tokens",
            "stop_sequences",
            "inference_config_json",
            "description",
        )
        flag_options = (
            "create_new",
            "clear_system_prompt",
            "combine_prompts",
            "clear_inference_config",
            "is_active",
            "is_inactive",
        )
        if any(options.get(option) is not None for option in value_options) or any(
            options.get(option) for option in flag_options
        ):
            raise CommandError("--list or --list-with-prompts must be used alone.")

    @staticmethod
    def _validate_options(options: dict) -> None:
        if options.get("is_active") and options.get("is_inactive"):
            raise CommandError("Cannot specify both --is-active and --is-inactive.")
        if options.get("combine_prompts") and not (
            options.get("system_prompt_id") is not None or options.get("new_system_prompt") is not None
        ):
            raise CommandError("--combine-prompts requires --system-prompt-id and/or --new-system-prompt.")
        if options.get("new_prompt_name") is not None and not (
            options.get("new_system_prompt") is not None or options.get("combine_prompts")
        ):
            raise CommandError("--new-prompt-name can only be used when creating a new prompt.")
        if options.get("create_new") and options.get("pk") is not None:
            raise CommandError("Cannot use --pk with --create-new.")
        if options.get("create_new") and options.get("model_id") is None and options.get("model_name") is None:
            raise CommandError("Creating an AI Assistant requires --model-id or --model-name.")

    @staticmethod
    def _has_updates(options: dict) -> bool:
        value_options = (
            "model_id",
            "model_name",
            "system_prompt_id",
            "new_system_prompt",
            "new_prompt_name",
            "temperature",
            "max_tokens",
            "top_p",
            "stop_sequences",
            "inference_config_json",
            "description",
        )
        flag_options = (
            "clear_system_prompt",
            "combine_prompts",
            "clear_inference_config",
            "is_active",
            "is_inactive",
        )
        return any(options.get(option) is not None for option in value_options) or any(
            options.get(option) for option in flag_options
        )

    def _apply_updates(self, assistant: Assistant, options: dict) -> None:
        # Clear all current system prompts if specified.
        if options.get("clear_system_prompt"):
            assistant.system_prompt_id = None

        # Update AI Assistant configurations:
        # 1. Update AI Model to use (if provided).
        if options.get("model_id") is not None or options.get("model_name") is not None:
            assistant.ai_model_id = self._get_model_pk(options)
        # 2. Update system prompt (if prompt options provided).
        if options.get("system_prompt_id") is not None or options.get("new_system_prompt") is not None:
            assistant.system_prompt_id = self._get_prompt_pk(assistant, options)
        # 3. Update inference configs (if provided).
        if any(
            options.get(option) is not None
            for option in ("temperature", "max_tokens", "top_p", "stop_sequences", "inference_config_json")
        ) or options.get("clear_inference_config"):
            assistant.inference_config = self._update_inference_configs(assistant, options)
        # 4. Update the assistant's active state (if provided).
        if options.get("is_active") or options.get("is_inactive"):
            active_state = self._update_active_state(options, assistant=assistant)
            if active_state is not None:
                assistant.is_active = active_state
        # 5. Update the assistant's description (if provided).
        if options.get("description") is not None:
            assistant.description = options.get("description")
        # 6. Save all changes.
        assistant.save()

    def _create_assistant(self, options: dict) -> Assistant:
        """Create a new AI Assistant with the specified name and configurations."""
        if options.get("model_id") is None and options.get("model_name") is None:
            raise CommandError("Creating an AI Assistant requires --model-id or --model-name.")
        model_pk = self._get_model_pk(options)
        # Deactivate the current active assistant with this name before creating a replacement.
        if options.get("is_active"):
            active_state = self._update_active_state(options)
        else:
            active_state = False
        assistant = Assistant.objects.create(
            name=options.get("name"),
            ai_model_id=model_pk,
            system_prompt_id=None,
            inference_config={},
            is_active=active_state,
            description=options.get("description") or "",
        )
        has_configs = False
        # Update the system prompt if specified.
        if options.get("system_prompt_id") is not None or options.get("new_system_prompt") is not None:
            has_configs = True
            assistant.system_prompt_id = self._get_prompt_pk(assistant, options)
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

    def _list_assistants(self, prompts: bool = False) -> None:
        """List all Assistants and their configs."""
        assistants = Assistant.objects.select_related("ai_model", "system_prompt").all()

        if not assistants.exists():
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

    def _get_assistant(self, options: dict) -> Assistant:
        """Retrieve AI Assistant."""
        name = options.get("name")
        pk = options.get("pk")
        # Selection logic:
        # 1. If neither pk nor name is provided, raise an error.
        # 2. If pk is provided, retrieve by pk.
        # 3. If name is provided and no pk, retrieve by name where is_active is also true.
        # 4. If name is provided and no pk, but no active assistant found, raise an error.
        if pk is None and not name:
            raise CommandError("Must specify an AI Assistant to retrieve with --name or --pk")
        if pk is not None and name:
            raise CommandError("Specify either --name or --pk, not both.")
        if pk is not None:
            try:
                assistant = Assistant.objects.get(pk=pk)
            except Assistant.DoesNotExist:
                raise CommandError(f"AI Assistant with pk '{pk}' not found.") from None
            return self._require_model(assistant)
        try:
            assistant = Assistant.objects.get(name=name, is_active=True)
        except Assistant.DoesNotExist:
            raise CommandError(f"Active AI Assistant with name '{name}' not found.") from None
        return self._require_model(assistant)

    @staticmethod
    def _require_model(assistant: Assistant) -> Assistant:
        if assistant.ai_model_id is None:
            raise CommandError(f"AI Assistant with pk '{assistant.pk}' has no AI model assigned.")
        return assistant

    def _get_model_pk(self, options: dict) -> int:
        """Retrieve model by ID or name."""
        model_id = options.get("model_id")
        model_name = options.get("model_name")

        # Gives preference to model_id.
        try:
            if model_id is not None:
                model = AIModel.objects.get(model_id=model_id)
            else:
                model = AIModel.objects.get(name=model_name)
            return model.pk
        except AIModel.DoesNotExist:
            raise CommandError(f"Model not found: {model_id or model_name}.") from None
        except AIModel.MultipleObjectsReturned:
            raise CommandError(f"Multiple models found for: {model_id or model_name}.") from None

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
        current_prompt_pk = assistant.system_prompt_id or 0
        # The final prompt primary key to return.
        return_prompt_pk = assistant.system_prompt_id
        # The system prompt specified by --system-prompt-id.
        existing_prompt_pk = options.get("system_prompt_id")
        if existing_prompt_pk is not None and existing_prompt_pk < 1:
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
                new_prompt_name,
            )
        # If prompts ARE being combined:
        else:
            return_prompt_pk = self._handle_combine_prompts(
                current_prompt_pk,
                existing_prompt_pk,
                new_prompt,
                new_prompt_name
            )
        return return_prompt_pk

    def _handle_swap_prompts(
        self,
        current_prompt_pk: int,
        existing_prompt_pk: int | None,
        new_prompt: str | None,
        new_prompt_name: str | None,
    ) -> int | None:
        """Handles swapping system prompts as specified by command options."""
        return_prompt_pk = current_prompt_pk or None
        # If --new-system-prompt and --system-prompt-id provided without --combine-prompts flag, raise an error.
        if new_prompt is not None and existing_prompt_pk is not None:
            raise CommandError("Cannot use --new-system-prompt and --system-prompt-id without --combine-prompts")
        # Keep the same prompt if specified prompt is what's in use and prompts are not being combined.
        if existing_prompt_pk is not None:
            try:
                Prompts.objects.get(pk=existing_prompt_pk)
            except Prompts.DoesNotExist:
                raise CommandError(f"System prompt with pk '{existing_prompt_pk}' not found.") from None
            if existing_prompt_pk == current_prompt_pk:
                logger.info("Specified prompt is the same as the one currently in use. No changes made to prompt.")
            else:
                return_prompt_pk = existing_prompt_pk
        # If --new-system-prompt is provided and --combine-prompts is not, create a new prompt and return its pk.
        elif new_prompt is not None:
            return_prompt_pk = self._create_new_prompt([new_prompt], new_prompt_name)
        return return_prompt_pk

    def _handle_combine_prompts(
        self,
        current_prompt_pk: int,
        existing_prompt_pk: int | None,
        new_prompt: str | None,
        new_prompt_name: str | None,
    ) -> int:
        """Handles combining system prompts as specified by command options."""
        # Get the text of the prompt currently in use.
        if current_prompt_pk > 0:
            try:
                current_prompt = Prompts.objects.get(pk=current_prompt_pk)
            except Prompts.DoesNotExist:
                raise CommandError(f"Current system prompt with pk '{current_prompt_pk}' not found.") from None
            current_prompt_text = current_prompt.text
        else:
            current_prompt_text = ""
        # Get the text of the new prompt being created via flag.
        new_prompt_text = new_prompt or ""
        # Combine the prompts.
        if existing_prompt_pk is not None:
            try:
                existing_prompt = Prompts.objects.get(pk=existing_prompt_pk)
            except Prompts.DoesNotExist:
                raise CommandError(f"System prompt with pk '{existing_prompt_pk}' not found.") from None
            existing_prompt_text = existing_prompt.text
            combined_prompt_text = [current_prompt_text, existing_prompt_text, new_prompt_text]
        else:
            combined_prompt_text = [current_prompt_text, new_prompt_text]
        # Create a new prompt with the combined text.
        return_prompt_pk = self._create_new_prompt(combined_prompt_text, new_prompt_name)
        return return_prompt_pk

    def _create_new_prompt(self, text: list, new_prompt_name: str | None) -> int:
        """
        Create a new system prompt with the given list of text. Prompt name will be a random UUID if name not specified.

        Args:
            text: List of strings to be joined with newlines.

        Returns:
            int: The primary key of the newly created prompt.
        """
        # Filter out empty strings to avoid extra newlines.
        filtered_text = [t for t in text if t]
        if not any(t.strip() for t in filtered_text):
            raise CommandError("Prompt text cannot be empty.")
        prompt_name = new_prompt_name or str(uuid.uuid4())
        if new_prompt_name is not None:
            if not new_prompt_name.strip():
                raise CommandError("Prompt name cannot be empty.")
            if len(new_prompt_name) > 100:
                raise CommandError("Prompt name cannot exceed 100 characters.")
            if Prompts.objects.filter(name=new_prompt_name).exists():
                raise CommandError(f"A prompt named '{new_prompt_name}' already exists.")
        # Craft the new Prompt to be created in the DB.
        try:
            with transaction.atomic():
                prompt = Prompts.objects.create(
                    name=prompt_name,
                    description="Custom prompt created by management command: 'manage_llm_assistant'",
                    text="\n".join(filtered_text),
                )
        except IntegrityError:
            raise CommandError(f"A prompt named '{prompt_name}' already exists.") from None
        return prompt.pk

    def _update_inference_configs(self, assistant: Assistant, options: dict) -> dict:
        """Takes the command input and produces a new dictionary of inference configs."""
        # Handle --clear-inference-config flag.
        if options.get("clear_inference_config"):
            if options.get("inference_config_json") is not None or any(
                options.get(option) is not None for option in ("temperature", "max_tokens", "top_p", "stop_sequences")
            ):
                raise CommandError("Cannot combine --clear-inference-config with other inference config options.")
            return {}
        # Get the current config object.
        current_config = assistant.inference_config or {}
        # Handle edge case of individual configs and full configs getting provided together in one command:
        individual_config_options = ("temperature", "max_tokens", "top_p", "stop_sequences")
        if options.get("inference_config_json") is not None and any(
            options.get(option) is not None for option in individual_config_options
        ):
            raise CommandError(
                "Cannot provide both individual inference config options and a full inference config JSON string."
            )
        # If config options are being changed individually and not as a JSON string:
        if options.get("inference_config_json") is None:
            new_config = self._handle_individual_inference_configs(options, current_config)
        # If a JSON string is provided for the entire config dict:
        else:
            new_config = self._handle_inference_config_json(options, current_config)
        return new_config if new_config else current_config

    def _handle_individual_inference_configs(self, options: dict, current_config: dict) -> dict:
        # Handle each field of the inference config separately.
        temperature = options.get("temperature")
        max_tokens = options.get("max_tokens")
        top_p = options.get("top_p")
        stop_sequences = options.get("stop_sequences")
        if stop_sequences is not None:
            stop_sequences = [sequence.strip() for sequence in stop_sequences]
            if any(not sequence for sequence in stop_sequences):
                raise CommandError("Stop sequences cannot be empty.")
        default_config = InferenceConfig().model_dump()
        new_config = {
            "temperature": temperature if temperature is not None else current_config.get(
                "temperature", default_config["temperature"]
            ),
            "maxTokens": max_tokens if max_tokens is not None else current_config.get(
                "maxTokens", default_config["maxTokens"]
            ),
            "topP": top_p if top_p is not None else current_config.get("topP", default_config["topP"]),
            "stopSequences": stop_sequences
            if stop_sequences is not None
            else current_config.get("stopSequences", default_config["stopSequences"]),
        }
        # Validate the new config using Pydantic model.
        try:
            InferenceConfig(**new_config)
        except ValidationError as e:
            raise CommandError(f"Invalid inference config: {e}") from e
        return new_config

    def _handle_inference_config_json(self, options: dict, current_config: dict) -> dict:
        new_config_json = options.get("inference_config_json")
        # Validate the JSON config using Pydantic model.
        try:
            config_dict = json.loads(str(new_config_json))
            if not isinstance(config_dict, dict):
                raise CommandError("Inference config JSON must be an object.")
            # Merge with current config to allow partial updates from inference_config_json.
            # Missing values use the Assistant's application defaults unless explicitly set to null.
            # Example: --inference-config-json '{"temperature": 0.7, "maxTokens": 1000}'
            # ^^ This only updates those 2 values; existing values remain unchanged and missing values use defaults.
            # To let an AI model use its own defaults, set values to null.
            # Example: --inference-config-json '{"maxTokens": null, "stopSequences": null}'
            # ^^ Removes those parameters from the request, allowing the Bedrock LLM to use its own defaults.
            default_config = InferenceConfig().model_dump()
            merged_config = {**default_config, **current_config, **config_dict}
            InferenceConfig(**merged_config)
            new_config = merged_config
        except ValidationError as e:
            raise CommandError(f"Invalid inference config: {e}") from e
        except json.JSONDecodeError as e:
            raise CommandError(f"Invalid JSON in inference config: {e}") from e
        return new_config

    def _update_active_state(self, options: dict, assistant: Assistant | None = None) -> bool | None:
        """Update the active state of the assistant."""
        is_active = options.get("is_active")
        is_inactive = options.get("is_inactive")

        if is_active and is_inactive:
            raise CommandError("Cannot specify both --is-active and --is-inactive.")

        if is_active:
            # Deactivate any currently active assistants with the same name, excluding the target.
            assistant_name = assistant.name if assistant else options.get("name")
            active_assistants = Assistant.objects.filter(name=assistant_name, is_active=True)
            if assistant:
                active_assistants = active_assistants.exclude(pk=assistant.pk)
            active_assistants.update(is_active=False)
            return True
        elif is_inactive:
            return False
        else:
            return None
