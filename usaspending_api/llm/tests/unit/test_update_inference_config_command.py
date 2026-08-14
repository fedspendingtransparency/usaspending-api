import json
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from usaspending_api.llm.models.db_models import AIModel


@pytest.mark.django_db
class TestTemperatureParameter:
    def test_update_temperature_valid(self):
        """Test updating temperature with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--temperature", "0.5")

        model.refresh_from_db()
        assert model.inference_config["temperature"] == 0.5

    def test_update_temperature_invalid_too_high(self):
        """Test that temperature > 1.0 raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid temperature.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--temperature", "1.5")

    def test_update_temperature_invalid_negative(self):
        """Test that negative temperature raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid temperature.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--temperature", "-0.1")

    def test_boundary_values_temperature(self):
        """Test boundary values for temperature (0.0 and 1.0)."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--temperature", "0.0")
        model.refresh_from_db()
        assert model.inference_config["temperature"] == 0.0

        call_command("update_inference_config", "--model-name", "test model", "--temperature", "1.0")
        model.refresh_from_db()
        assert model.inference_config["temperature"] == 1.0


@pytest.mark.django_db
class TestTopPParameter:
    def test_update_top_p_valid(self):
        """Test updating topP with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--top-p", "0.8")

        model.refresh_from_db()
        assert model.inference_config["topP"] == 0.8

    def test_update_top_p_invalid_too_high(self):
        """Test that topP > 1.0 raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid top-p.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--top-p", "2.0")

    def test_update_top_p_invalid_negative(self):
        """Test that negative topP raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid top-p.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--top-p", "-0.5")

    def test_boundary_values_top_p(self):
        """Test boundary values for topP (0.0 and 1.0)."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--top-p", "0.0")
        model.refresh_from_db()
        assert model.inference_config["topP"] == 0.0

        call_command("update_inference_config", "--model-name", "test model", "--top-p", "1.0")
        model.refresh_from_db()
        assert model.inference_config["topP"] == 1.0


@pytest.mark.django_db
class TestMaxTokensParameter:
    def test_update_max_tokens_valid(self):
        """Test updating maxTokens with valid value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--max-tokens", "4096")

        model.refresh_from_db()
        assert model.inference_config["maxTokens"] == 4096

    def test_update_max_tokens_invalid_zero(self):
        """Test that maxTokens = 0 raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid max-tokens.*Must be a positive integer"):
            call_command("update_inference_config", "--model-name", "test model", "--max-tokens", "0")

    def test_update_max_tokens_invalid_negative(self):
        """Test that negative maxTokens raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        with pytest.raises(CommandError, match=r"Invalid max-tokens.*Must be a positive integer"):
            call_command("update_inference_config", "--model-name", "test model", "--max-tokens", "-100")


@pytest.mark.django_db
class TestStopSequencesParameter:
    def test_update_stop_sequences_single(self):
        """Test updating stopSequences with single value."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--stop-sequences", "Human:")

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == ["Human:"]

    def test_update_stop_sequences_multiple(self):
        """Test updating stopSequences with multiple values"""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command(
            "update_inference_config", "--model-name", "test model", "--stop-sequences", "Human:,User:,Assistant:"
        )

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == ["Human:", "User:", "Assistant:"]

    def test_update_stop_sequences_with_newlines(self):
        """Test updating stopSequences with escaped newlines."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command("update_inference_config", "--model-name", "test model", "--stop-sequences", "\\n\\n,Human:")

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == ["\n\n", "Human:"]

    def test_update_stop_sequences_empty_string(self):
        """Test updating stopSequences with empty string results in empty list."""
        model = AIModel.objects.create(
            name="test model", model_id="test-id", provider="test", inference_config={"stopSequences": ["old"]}
        )

        call_command("update_inference_config", "--model-name", "test model", "--stop-sequences", "")

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == []

    def test_update_stop_sequences_with_spaces(self):
        """Test that spaces around stop sequences are stripped."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command(
            "update_inference_config", "--model-name", "test model", "--stop-sequences", " Human: , User: , Assistant: "
        )

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == ["Human:", "User:", "Assistant:"]


@pytest.mark.django_db
class TestJsonConfigUpdates:
    def test_update_config_json_valid(self):
        """Test updating with valid JSON config."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.7, "topP": 0.9, "maxTokens": 2048})

        call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

        model.refresh_from_db()
        assert model.inference_config["temperature"] == 0.7
        assert model.inference_config["topP"] == 0.9
        assert model.inference_config["maxTokens"] == 2048

    def test_update_config_json_invalid_temperature(self):
        """Test that JSON with invalid temperature raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 1.5, "topP": 0.9, "maxTokens": 2048})

        with pytest.raises(CommandError, match=r"Invalid temperature.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_invalid_top_p(self):
        """Test that JSON with invalid topP raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.5, "topP": 1.1, "maxTokens": 2048})

        with pytest.raises(CommandError, match=r"Invalid top-p.*Must be between 0.0 and 1.0"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_invalid_max_tokens(self):
        """Test that JSON with invalid maxTokens raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.5, "topP": 0.9, "maxTokens": -100})

        with pytest.raises(CommandError, match=r"Invalid max-tokens.*Must be a positive integer"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_invalid_type_temperature(self):
        """Test that JSON with wrong type for temperature raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": "high", "topP": 0.9, "maxTokens": 2048})

        with pytest.raises(CommandError, match=r"Invalid temperature type.*Must be a number"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_invalid_type_max_tokens(self):
        """Test that JSON with wrong type for maxTokens raises error"""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.5, "topP": 0.9, "maxTokens": 2048.5})

        with pytest.raises(CommandError, match=r"Invalid maxTokens type.*Must be an integer"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_with_stop_sequences(self):
        """Test updating with JSON config including stopSequences."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps(
            {"temperature": 0.5, "topP": 0.8, "maxTokens": 2048, "stopSequences": ["Human:", "User:"]}
        )

        call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

        model.refresh_from_db()
        assert model.inference_config["stopSequences"] == ["Human:", "User:"]

    def test_update_config_json_invalid_stop_sequences_not_list(self):
        """Test that JSON with non-list stopSequences raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.5, "stopSequences": "Human:"})

        with pytest.raises(CommandError, match=r"Invalid stop-sequences type.*Must be a list"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)

    def test_update_config_json_invalid_stop_sequences_non_string_element(self):
        """Test that JSON with non-string elements in stopSequences raises error."""
        AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        config_json = json.dumps({"temperature": 0.5, "stopSequences": ["Human:", 123, "User:"]})

        with pytest.raises(CommandError, match=r"Invalid stop sequence at index 1.*All stop sequences must be strings"):
            call_command("update_inference_config", "--model-name", "test model", "--config-json", config_json)


@pytest.mark.django_db
class TestMultipleParametersAndBoundaries:
    def test_update_multiple_params_valid(self):
        """Test updating multiple parameters at once."""
        model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        call_command(
            "update_inference_config",
            "--model-name",
            "test model",
            "--temperature",
            "0.3",
            "--top-p",
            "0.7",
            "--max-tokens",
            "1024",
        )

        model.refresh_from_db()
        assert model.inference_config["temperature"] == 0.3
        assert model.inference_config["topP"] == 0.7
        assert model.inference_config["maxTokens"] == 1024


@pytest.mark.django_db
class TestCommandOperations:
    def test_list_command(self):
        """Test list command displays models and configs."""
        AIModel.objects.create(
            name="model 1", model_id="model-1", provider="test", inference_config={"temperature": 0.5}
        )
        AIModel.objects.create(name="model 2", model_id="model-2", provider="test")

        out = StringIO()
        call_command("update_inference_config", "--list", stdout=out)
        output = out.getvalue()

        assert "model 1" in output
        assert "model 2" in output
        assert "temperature: 0.5" in output

    def test_clear_command(self):
        """Test clear command removes inference config."""
        model = AIModel.objects.create(
            name="test model", model_id="test-id", provider="test", inference_config={"temperature": 0.5, "topP": 0.8}
        )

        call_command("update_inference_config", "--model-name", "test model", "--clear")

        model.refresh_from_db()
        assert model.inference_config == {}
