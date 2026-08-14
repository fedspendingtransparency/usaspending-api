from unittest.mock import Mock, patch

import pytest

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import AIModel, Session


@pytest.mark.django_db
class TestInferenceConfig:
    def test_assistant_uses_model_inference_config(self):
        """Test that FilterSearchAssistant uses model's inference config."""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test",
            inference_config={
                "temperature": 0.8,
                "topP": 0.9,
                "maxTokens": 512,
            },
        )

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)

        assert assistant.inference_config == {
            "temperature": 0.8,
            "topP": 0.9,
            "maxTokens": 512,
        }

    def test_assistant_uses_defaults_when_no_config(self):
        """Test that assistant uses defaults when model has no inference config."""
        ai_model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)

        assert assistant.inference_config == {
            "temperature": 0.0,
            "topP": 1.0,
            "maxTokens": 2048,
        }

    def test_assistant_uses_defaults_when_empty_config(self):
        """Test that assistant uses defaults when model has empty inference config."""
        ai_model = AIModel.objects.create(name="test model", model_id="test-id", provider="test", inference_config={})

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)

        assert assistant.inference_config == {
            "temperature": 0.0,
            "topP": 1.0,
            "maxTokens": 2048,
        }

    def test_different_models_different_configs(self):
        """Test that different models can have different inference configs."""
        model_deterministic = AIModel.objects.create(
            name="deterministic model",
            model_id="det-id",
            provider="test",
            inference_config={
                "temperature": 0.0,
                "topP": 0.1,
                "maxTokens": 2048,
            },
        )

        model_creative = AIModel.objects.create(
            name="creative model",
            model_id="creative-id",
            provider="test",
            inference_config={
                "temperature": 0.9,
                "topP": 0.95,
                "maxTokens": 4096,
            },
        )

        session1 = Session.objects.create(ai_model=model_deterministic)
        session2 = Session.objects.create(ai_model=model_creative)

        with patch("boto3.client"):
            assistant1 = FilterSearchAssistant(model=model_deterministic, tools=[], session=session1)
            assistant2 = FilterSearchAssistant(model=model_creative, tools=[], session=session2)

        assert assistant1.inference_config["temperature"] == 0.0
        assert assistant2.inference_config["temperature"] == 0.9

    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_inference_config_passed_to_bedrock(self, mock_message_create):
        """Test that inference config is passed to Bedrock converse API."""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test",
            inference_config={
                "temperature": 0.5,
                "topP": 0.7,
                "maxTokens": 1024,
            },
        )

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client") as mock_boto_client:
            mock_client = Mock()
            mock_boto_client.return_value = mock_client
            mock_client.converse.return_value = {
                "output": {"message": {"role": "assistant", "content": [{"text": "Response"}]}},
                "usage": {"inputTokens": 10, "outputTokens": 20},
                "metrics": {"latencyMs": 100},
                "stopReason": "end_turn",
            }

            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)
            assistant.client = mock_client

            list(assistant.search("test query"))

            mock_client.converse.assert_called_once()
            call_kwargs = mock_client.converse.call_args[1]

            assert "inferenceConfig" in call_kwargs
            assert call_kwargs["inferenceConfig"]["temperature"] == 0.5
            assert call_kwargs["inferenceConfig"]["topP"] == 0.7
            assert call_kwargs["inferenceConfig"]["maxTokens"] == 1024

    def test_assistant_with_stop_sequences(self):
        """Test that assistant uses stopSequences from model config."""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test",
            inference_config={
                "temperature": 0.0,
                "topP": 0.1,
                "maxTokens": 2048,
                "stopSequences": ["Human:", "User:"],
            },
        )

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)

        assert assistant.inference_config["stopSequences"] == ["Human:", "User:"]

    def test_assistant_defaults_include_stop_sequences(self):
        """Test that default config includes empty stopSequences."""
        ai_model = AIModel.objects.create(name="test model", model_id="test-id", provider="test")

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)

        assert "stopSequences" in assistant.inference_config
        assert assistant.inference_config["stopSequences"] == []

    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_stop_sequences_passed_to_bedrock(self, mock_message_create):
        """Test that stopSequences is passed to Bedrock converse API."""
        ai_model = AIModel.objects.create(
            name="test model",
            model_id="test-id",
            provider="test",
            inference_config={
                "temperature": 0.5,
                "topP": 0.8,
                "maxTokens": 2048,
                "stopSequences": ["Human:", "\n\nUser:"],
            },
        )

        session = Session.objects.create(ai_model=ai_model)

        with patch("boto3.client") as mock_boto_client:
            mock_client = Mock()
            mock_boto_client.return_value = mock_client
            mock_client.converse.return_value = {
                "output": {"message": {"role": "assistant", "content": [{"text": "Response"}]}},
                "usage": {"inputTokens": 10, "outputTokens": 20},
                "metrics": {"latencyMs": 100},
                "stopReason": "end_turn",
            }

            assistant = FilterSearchAssistant(model=ai_model, tools=[], session=session)
            assistant.client = mock_client

            list(assistant.search("test query"))

            mock_client.converse.assert_called_once()
            call_kwargs = mock_client.converse.call_args[1]

            assert "inferenceConfig" in call_kwargs
            assert call_kwargs["inferenceConfig"]["stopSequences"] == ["Human:", "\n\nUser:"]
