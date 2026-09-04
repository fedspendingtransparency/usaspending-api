import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from usaspending_api.llm.models.db_models import AIModel, Assistant, Prompts


@pytest.mark.django_db
class TestListAssistants:
    def test_list_assistants_empty(self, caplog):
        """Test listing assistants when none exist."""
        with caplog.at_level("INFO"):
            call_command("manage_llm_assistant", "--list")
        assert "No AI Assistants found" in caplog.text

    def test_list_assistants_basic(self, caplog):
        """Test listing assistants with basic format."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt = Prompts.objects.create(name="test prompt", description="Test", text="You are helpful")
        Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt,
            inference_config={"temperature": 0.5, "topP": 0.9, "maxTokens": 1000, "stopSequences": []},
        )

        with caplog.at_level("INFO"):
            call_command("manage_llm_assistant", "--list")
        assert "test-assistant" in caplog.text
        assert "test model" in caplog.text

    def test_list_assistants_with_prompts(self, caplog):
        """Test listing assistants with full prompt text (longer than 50 chars)."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        long_prompt_text = (
            "You are a helpful assistant that provides detailed and accurate information. "
            "Always be polite and professional in your responses. This prompt is intentionally "
            "longer than 50 characters to test the --list-with-prompts flag."
        )
        prompt = Prompts.objects.create(name="test prompt", description="Test", text=long_prompt_text)
        Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt,
            inference_config={"temperature": 0.5},
        )

        # Test that --list truncates the prompt
        caplog.clear()
        with caplog.at_level("INFO"):
            call_command("manage_llm_assistant", "--list")
        assert "..." in caplog.text  # Should show truncation
        assert "intentionally longer than 50 characters" not in caplog.text  # End of prompt should be truncated

        # Test that --list-with-prompts shows full prompt
        caplog.clear()
        with caplog.at_level("INFO"):
            call_command("manage_llm_assistant", "--list-with-prompts")
        assert long_prompt_text in caplog.text  # Full prompt should be visible

    def test_list_options_are_mutually_exclusive(self):
        with pytest.raises(CommandError, match=r"either --list or --list-with-prompts"):
            call_command("manage_llm_assistant", "--list", "--list-with-prompts")

    def test_list_must_be_used_alone(self):
        with pytest.raises(CommandError, match=r"--list or --list-with-prompts must be used alone"):
            call_command("manage_llm_assistant", "--list", "--name", "test-assistant")


@pytest.mark.django_db
class TestGetAssistant:
    def test_get_assistant_missing_name(self):
        """Test that missing --name raises error."""
        with pytest.raises(CommandError, match=r"Must specify an AI Assistant to retrieve"):
            call_command("manage_llm_assistant", "--temperature", "0.5")

    def test_get_assistant_not_found(self):
        """Test that non-existent assistant raises error."""
        with pytest.raises(CommandError, match=r"Active AI Assistant with name 'nonexistent' not found"):
            call_command("manage_llm_assistant", "--name", "nonexistent", "--temperature", "0.5")

    def test_get_assistant_no_updates(self):
        """Test that no update options raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"No update options provided"):
            call_command("manage_llm_assistant", "--name", "test-assistant")

    def test_new_prompt_name_requires_prompt_creation(self):
        with pytest.raises(CommandError, match=r"--new-prompt-name can only be used"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--new-prompt-name", "Prompt name")


@pytest.mark.django_db
class TestUpdateInferenceConfigTemperature:
    def test_update_temperature_valid(self):
        """Test updating temperature with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "0.5")

        assistant.refresh_from_db()
        assert assistant.inference_config == {
            "temperature": 0.5,
            "topP": 1.0,
            "maxTokens": 5000,
            "stopSequences": [],
        }

    def test_update_temperature_preserves_other_configs(self):
        """Test that updating temperature preserves other config values."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            inference_config={"temperature": 0.0, "topP": 0.9, "maxTokens": 2000, "stopSequences": ["END"]},
        )

        call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "0.7")

        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.7
        assert assistant.inference_config["topP"] == 0.9
        assert assistant.inference_config["maxTokens"] == 2000
        assert assistant.inference_config["stopSequences"] == ["END"]

    def test_update_temperature_boundary_values(self):
        """Test temperature boundary values (0.0 and 1.0)."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "0.0")
        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.0

        call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "1.0")
        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 1.0

    def test_update_temperature_invalid_too_high(self):
        """Test that temperature > 1.0 raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "1.5")

    def test_update_temperature_invalid_negative(self):
        """Test that negative temperature raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--temperature", "-0.1")


@pytest.mark.django_db
class TestUpdateInferenceConfigTopP:
    def test_update_top_p_valid(self):
        """Test updating topP with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--top-p", "0.8")

        assistant.refresh_from_db()
        assert assistant.inference_config["topP"] == 0.8

    def test_update_top_p_boundary_values(self):
        """Test topP boundary values (0.0 and 1.0)."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--top-p", "0.0")
        assistant.refresh_from_db()
        assert assistant.inference_config["topP"] == 0.0

        call_command("manage_llm_assistant", "--name", "test-assistant", "--top-p", "1.0")
        assistant.refresh_from_db()
        assert assistant.inference_config["topP"] == 1.0

    def test_update_top_p_invalid_too_high(self):
        """Test that topP > 1.0 raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--top-p", "2.0")

    def test_update_top_p_invalid_negative(self):
        """Test that negative topP raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--top-p", "-0.5")


@pytest.mark.django_db
class TestUpdateInferenceConfigMaxTokens:
    def test_update_max_tokens_valid(self):
        """Test updating maxTokens with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--max-tokens", "4096")

        assistant.refresh_from_db()
        assert assistant.inference_config["maxTokens"] == 4096

    def test_update_max_tokens_invalid_zero(self):
        """Test that maxTokens = 0 raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--max-tokens", "0")

    def test_update_max_tokens_invalid_negative(self):
        """Test that negative maxTokens raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--max-tokens", "-100")


@pytest.mark.django_db
class TestUpdateInferenceConfigStopSequences:
    def test_update_stop_sequences_single(self):
        """Test updating stopSequences with single value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command("manage_llm_assistant", "--name", "test-assistant", "--stop-sequences", "Human:")

        assistant.refresh_from_db()
        assert assistant.inference_config["stopSequences"] == ["Human:"]

    def test_update_stop_sequences_multiple(self):
        """Test updating stopSequences with multiple values."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--stop-sequences",
            "Human:,User:",
            "--stop-sequences",
            "Assistant:",
        )

        assistant.refresh_from_db()
        assert assistant.inference_config["stopSequences"] == ["Human:,User:", "Assistant:"]


@pytest.mark.django_db
class TestUpdateInferenceConfigMultipleParams:
    def test_update_multiple_params_together(self):
        """Test updating multiple inference parameters at once."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--temperature",
            "0.7",
            "--top-p",
            "0.9",
            "--max-tokens",
            "8192",
        )

        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.7
        assert assistant.inference_config["topP"] == 0.9
        assert assistant.inference_config["maxTokens"] == 8192


@pytest.mark.django_db
class TestUpdateInferenceConfigJSON:
    def test_update_with_json_config(self):
        """Test updating inference config with JSON string."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        config_json = '{"temperature": 0.8, "topP": 0.95, "maxTokens": 8192, "stopSequences": ["Human:", "User:"]}'
        call_command("manage_llm_assistant", "--name", "test-assistant", "--inference-config-json", config_json)

        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.8
        assert assistant.inference_config["topP"] == 0.95
        assert assistant.inference_config["maxTokens"] == 8192
        assert assistant.inference_config["stopSequences"] == ["Human:", "User:"]

    def test_update_with_json_config_on_empty_assistant_uses_defaults(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, inference_config={})

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--inference-config-json",
            '{"temperature": 0.7}',
        )

        assistant.refresh_from_db()
        assert assistant.inference_config == {
            "temperature": 0.7,
            "topP": 1.0,
            "maxTokens": 5000,
            "stopSequences": [],
        }

    def test_update_with_json_config_partial(self):
        """Test that JSON config merges with existing config."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            inference_config={"temperature": 0.0, "topP": 1.0, "maxTokens": 5000, "stopSequences": []},
        )

        config_json = '{"temperature": 0.7, "maxTokens": 1000}'
        call_command("manage_llm_assistant", "--name", "test-assistant", "--inference-config-json", config_json)

        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.7
        assert assistant.inference_config["topP"] == 1.0  # Preserved
        assert assistant.inference_config["maxTokens"] == 1000
        assert assistant.inference_config["stopSequences"] == []  # Preserved

    def test_update_with_json_config_null_values(self):
        """Test that null values in JSON config allow model defaults."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            inference_config={"temperature": 0.5, "topP": 0.9, "maxTokens": 2000, "stopSequences": ["END"]},
        )

        config_json = '{"temperature": 0.3, "topP": null, "maxTokens": null, "stopSequences": null}'
        call_command("manage_llm_assistant", "--name", "test-assistant", "--inference-config-json", config_json)

        assistant.refresh_from_db()
        assert assistant.inference_config["temperature"] == 0.3
        assert assistant.inference_config["topP"] is None
        assert assistant.inference_config["maxTokens"] is None
        assert assistant.inference_config["stopSequences"] is None

    def test_update_with_json_config_invalid_json(self):
        """Test that invalid JSON raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid JSON"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--inference-config-json",
                '{"temperature": invalid}',
            )

    def test_update_with_json_config_invalid_values(self):
        """Test that invalid values in JSON raise error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command(
                "manage_llm_assistant", "--name", "test-assistant", "--inference-config-json", '{"temperature": 2.0}'
            )

    def test_update_with_json_unknown_key_raises_error(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Invalid inference config"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--inference-config-json",
                '{"temperatur": 0.5}',
            )

    def test_update_with_json_non_object_raises_error(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Inference config JSON must be an object"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--inference-config-json",
                "[]",
            )

    def test_update_json_and_individual_params_raises_error(self):
        """Test that providing both JSON and individual params raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Cannot provide both individual inference config options"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--inference-config-json",
                '{"temperature": 0.5}',
                "--top-p",
                "0.9",
            )


@pytest.mark.django_db
class TestClearInferenceConfig:
    def test_clear_inference_config(self):
        """Test clearing inference config."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True,
            name="test-assistant",
            ai_model=model,
            inference_config={"temperature": 0.5, "topP": 0.9, "maxTokens": 2000, "stopSequences": ["END"]},
        )

        call_command("manage_llm_assistant", "--name", "test-assistant", "--clear-inference-config")

        assistant.refresh_from_db()
        assert assistant.inference_config == {}


@pytest.mark.django_db
class TestUpdateAIModel:
    def test_update_model_by_id(self):
        """Test updating AI model by model_id."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model1)

        call_command("manage_llm_assistant", "--name", "test-assistant", "--model-id", "model-2")

        assistant.refresh_from_db()
        assert assistant.ai_model == model2

    def test_update_model_by_name(self):
        """Test updating AI model by model name."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model1)

        call_command("manage_llm_assistant", "--name", "test-assistant", "--model-name", "model 2")

        assistant.refresh_from_db()
        assert assistant.ai_model == model2

    def test_update_model_id_takes_precedence_over_name(self):
        """Test that model-id takes precedence over model-name."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model1)

        call_command(
            "manage_llm_assistant", "--name", "test-assistant", "--model-id", "model-2", "--model-name", "model 1"
        )

        assistant.refresh_from_db()
        assert assistant.ai_model == model2

    def test_update_model_not_found_by_id(self):
        """Test that non-existent model_id raises error."""
        model = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Model not found: nonexistent"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--model-id", "nonexistent")

    def test_update_model_not_found_by_name(self):
        """Test that non-existent model name raises error."""
        model = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"Model not found: nonexistent"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--model-name", "nonexistent")


@pytest.mark.django_db
class TestUpdateSystemPrompt:
    def test_update_prompt_by_id(self):
        """Test updating system prompt by ID."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        prompt2 = Prompts.objects.create(name="prompt 2", description="Test", text="You are creative")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        call_command("manage_llm_assistant", "--name", "test-assistant", "--system-prompt-id", str(prompt2.pk))

        assistant.refresh_from_db()
        assert assistant.system_prompt == prompt2

    def test_update_prompt_with_new_text(self):
        """Test creating new prompt with --new-system-prompt."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        call_command("manage_llm_assistant", "--name", "test-assistant", "--new-system-prompt", "You are an expert")

        assistant.refresh_from_db()
        assert assistant.system_prompt is not None
        assert assistant.system_prompt.text == "You are an expert"
        assert "Custom Prompt" in assistant.system_prompt.name

    def test_update_prompt_combine_existing_and_new(self):
        """Test combining existing prompt with new text."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--new-system-prompt",
            "You are also creative",
            "--combine-prompts",
        )

        assistant.refresh_from_db()
        assert "You are helpful" in assistant.system_prompt.text
        assert "You are also creative" in assistant.system_prompt.text

    def test_update_prompt_combine_two_existing(self):
        """Test combining current prompt with another existing prompt."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        prompt2 = Prompts.objects.create(name="prompt 2", description="Test", text="You are creative")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--system-prompt-id",
            str(prompt2.pk),
            "--combine-prompts",
        )

        assistant.refresh_from_db()
        assert "You are helpful" in assistant.system_prompt.text
        assert "You are creative" in assistant.system_prompt.text

    def test_update_prompt_combine_all_three(self):
        """Test combining current, existing, and new prompts."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        prompt2 = Prompts.objects.create(name="prompt 2", description="Test", text="You are creative")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--system-prompt-id",
            str(prompt2.pk),
            "--new-system-prompt",
            "You are also precise",
            "--combine-prompts",
        )

        assistant.refresh_from_db()
        assert "You are helpful" in assistant.system_prompt.text
        assert "You are creative" in assistant.system_prompt.text
        assert "You are also precise" in assistant.system_prompt.text

    def test_update_prompt_without_combine_raises_error(self):
        """Test that using both --new-system-prompt and --system-prompt-id without --combine-prompts raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        prompt2 = Prompts.objects.create(name="prompt 2", description="Test", text="You are creative")
        Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        with pytest.raises(CommandError, match=r"Cannot use --new-system-prompt and --system-prompt-id without"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--system-prompt-id",
                str(prompt2.pk),
                "--new-system-prompt",
                "You are precise",
            )

    def test_update_prompt_same_as_current(self, caplog):
        """Test that specifying the same prompt as current logs info message."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt1,
        )

        with caplog.at_level("INFO"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--system-prompt-id", str(prompt1.pk))

        assert "same as the one currently in use" in caplog.text

    def test_clear_system_prompt(self):
        """Test clearing system prompt."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        prompt = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model,
            system_prompt=prompt,
        )

        call_command("manage_llm_assistant", "--name", "test-assistant", "--clear-system-prompt")

        assistant.refresh_from_db()
        assert assistant.system_prompt is None

    def test_update_prompt_invalid_id_negative(self):
        """Test that negative prompt ID raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"System prompt ID must be an integer greater than 0"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--system-prompt-id", "-1")

    def test_update_prompt_invalid_id_zero(self):
        """Test that zero prompt ID raises error."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model)

        with pytest.raises(CommandError, match=r"System prompt ID must be an integer greater than 0"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--system-prompt-id", "0")

    def test_combine_prompts_when_no_current_prompt(self):
        """Test combining prompts when assistant has no current prompt."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(is_active=True, name="test-assistant", ai_model=model, system_prompt=None)

        # This should handle the case where current_prompt is None
        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--new-system-prompt",
            "You are helpful",
            "--combine-prompts",
        )

        assistant.refresh_from_db()
        assert assistant.system_prompt is not None
        assert "You are helpful" in assistant.system_prompt.text

    def test_clear_current_prompt_before_combining(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        current_prompt = Prompts.objects.create(name="current prompt", description="Test", text="Current instructions")
        assistant = Assistant.objects.create(
            name="test-assistant",
            ai_model=model,
            system_prompt=current_prompt,
            is_active=True,
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--clear-system-prompt",
            "--new-system-prompt",
            "New instructions",
            "--combine-prompts",
        )

        assistant.refresh_from_db()
        assert assistant.system_prompt.text == "New instructions"
        assert "Current instructions" not in assistant.system_prompt.text

    def test_empty_new_prompt_is_rejected(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(name="test-assistant", ai_model=model, is_active=True)

        with pytest.raises(CommandError, match=r"Prompt text cannot be empty"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--new-system-prompt", "   ")

    def test_duplicate_new_prompt_name_is_rejected(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(name="test-assistant", ai_model=model, is_active=True)
        Prompts.objects.create(name="existing prompt", description="Test", text="Existing instructions")

        with pytest.raises(CommandError, match=r"A prompt named 'existing prompt' already exists"):
            call_command(
                "manage_llm_assistant",
                "--name",
                "test-assistant",
                "--new-system-prompt",
                "New instructions",
                "--new-prompt-name",
                "existing prompt",
            )


@pytest.mark.django_db
class TestCreateAndActivateAssistant:
    def test_create_requires_model(self):
        with pytest.raises(CommandError, match=r"requires --model-id or --model-name"):
            call_command("manage_llm_assistant", "--create-new", "--name", "test-assistant")

    def test_create_defaults_to_inactive_and_empty_description(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command(
            "manage_llm_assistant",
            "--create-new",
            "--name",
            "test-assistant",
            "--model-id",
            model.model_id,
        )

        assistant = Assistant.objects.get(name="test-assistant")
        assert assistant.ai_model == model
        assert assistant.is_active is False
        assert assistant.description == ""

    def test_create_active_deactivates_existing_active_assistant(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        existing = Assistant.objects.create(name="test-assistant", ai_model=model, is_active=True)

        call_command(
            "manage_llm_assistant",
            "--create-new",
            "--name",
            "test-assistant",
            "--model-id",
            model.model_id,
            "--is-active",
        )

        existing.refresh_from_db()
        replacement = Assistant.objects.get(name="test-assistant", is_active=True)
        assert existing.is_active is False
        assert replacement.pk != existing.pk

    def test_activate_inactive_assistant_by_pk_when_no_active_exists(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        assistant = Assistant.objects.create(name="test-assistant", ai_model=model, is_active=False)

        call_command("manage_llm_assistant", "--pk", str(assistant.pk), "--is-active")

        assistant.refresh_from_db()
        assert assistant.is_active is True

    def test_name_selection_requires_an_active_assistant(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        Assistant.objects.create(name="test-assistant", ai_model=model, is_active=False)

        with pytest.raises(CommandError, match=r"Active AI Assistant with name 'test-assistant' not found"):
            call_command("manage_llm_assistant", "--name", "test-assistant", "--is-active")

    def test_activate_by_pk_deactivates_other_active_same_name(self):
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")
        active_assistant = Assistant.objects.create(name="test-assistant", ai_model=model, is_active=True)
        inactive_assistant = Assistant.objects.create(name="test-assistant", ai_model=model, is_active=False)

        call_command("manage_llm_assistant", "--pk", str(inactive_assistant.pk), "--is-active")

        active_assistant.refresh_from_db()
        inactive_assistant.refresh_from_db()
        assert active_assistant.is_active is False
        assert inactive_assistant.is_active is True

    def test_active_flags_are_mutually_exclusive(self):
        with pytest.raises(CommandError, match=r"Cannot specify both --is-active and --is-inactive"):
            call_command(
                "manage_llm_assistant",
                "--create-new",
                "--name",
                "test-assistant",
                "--model-id",
                "test-model-id",
                "--is-active",
                "--is-inactive",
            )


@pytest.mark.django_db
class TestCombinedUpdates:
    def test_update_model_and_inference_config(self):
        """Test updating both model and inference config together."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model1,
            inference_config={},
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--model-name",
            "model 2",
            "--temperature",
            "0.7",
            "--max-tokens",
            "4096",
        )

        assistant.refresh_from_db()
        assert assistant.ai_model == model2
        assert assistant.inference_config["temperature"] == 0.7
        assert assistant.inference_config["maxTokens"] == 4096

    def test_update_model_prompt_and_inference_config(self):
        """Test updating model, prompt, and inference config together."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        prompt1 = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        prompt2 = Prompts.objects.create(name="prompt 2", description="Test", text="You are creative")
        assistant = Assistant.objects.create(is_active=True,
            name="test-assistant", ai_model=model1, system_prompt=prompt1, inference_config={}
        )

        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--model-name",
            "model 2",
            "--system-prompt-id",
            str(prompt2.pk),
            "--temperature",
            "0.8",
        )

        assistant.refresh_from_db()
        assert assistant.ai_model == model2
        assert assistant.system_prompt == prompt2
        assert assistant.inference_config["temperature"] == 0.8

    def test_update_all_components_with_json_config(self):
        """Test updating all components with JSON inference config."""
        model1 = AIModel.objects.create(name="model 1", model_id="model-1", provider="provider1")
        model2 = AIModel.objects.create(name="model 2", model_id="model-2", provider="provider2")
        prompt = Prompts.objects.create(name="prompt 1", description="Test", text="You are helpful")
        assistant = Assistant.objects.create(
            is_active=True,
            name="test-assistant",
            ai_model=model1,
            inference_config={},
        )

        config_json = '{"temperature": 0.9, "topP": 0.95, "maxTokens": 10000, "stopSequences": []}'
        call_command(
            "manage_llm_assistant",
            "--name",
            "test-assistant",
            "--model-id",
            "model-2",
            "--system-prompt-id",
            str(prompt.pk),
            "--inference-config-json",
            config_json,
        )

        assistant.refresh_from_db()
        assert assistant.ai_model == model2
        assert assistant.system_prompt == prompt
        assert assistant.inference_config["temperature"] == 0.9
        assert assistant.inference_config["topP"] == 0.95
        assert assistant.inference_config["maxTokens"] == 10000
