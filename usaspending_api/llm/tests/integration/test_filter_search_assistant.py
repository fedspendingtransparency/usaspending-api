from unittest.mock import Mock, patch

import pytest

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import AIModel, Session
from usaspending_api.llm.models.py_models import AITool


@pytest.fixture
def mock_session():
    session = Mock(spec=Session)
    session.id = "123"
    return session


@pytest.fixture
def mock_model():
    model = Mock(spec=AIModel)
    model.model_id = "test-model-id"
    return model


@pytest.fixture
def mock_tool():
    tool = Mock(spec=AITool)
    tool.description = Mock()
    tool.description.name = "test_tool"
    tool.description.model_dump = Mock(
        return_value={
            "name": "test_tool",
            "description": "A test tool",
            "input_schema": {"type": "object", "properties": {}},
        }
    )
    tool.function = Mock(return_value={"result": "success"})
    tool.logging = Mock(return_value="Executing test_tool")
    return tool


@pytest.fixture
def mock_search_tool():
    tool = Mock(spec=AITool)
    tool.description = Mock()
    tool.description.name = "execute_filter"
    tool.description.model_dump = Mock(
        return_value={
            "name": "execute_filter",
            "description": "Search tool",
            "input_schema": {"type": "object", "properties": {}},
        }
    )
    tool.function = Mock(return_value={"hash": "abc123", "results": []})
    tool.logging = Mock(return_value="Searching federal contracts")
    return tool


@pytest.fixture
def assistant(mock_model, mock_tool, mock_search_tool, mock_session):
    with patch("boto3.client"):
        assistant = FilterSearchAssistant(
            model=mock_model,
            tools=[mock_tool, mock_search_tool],
            session=mock_session,
            system_message="Test system message",
        )
        assistant.client = Mock()
        return assistant


class TestFilterSearchAssistant:

    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_search_simple_response(self, mock_message_create, assistant):
        """Test search with a simple text response (no tool use)."""
        mock_message = Mock()
        mock_message_create.return_value = mock_message

        assistant.client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Here's your answer"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "end_turn",
        }

        results = list(assistant.search("test query"))

        assert len(results) == 1
        assert results[0]["type"] == "search_start"
        assert assistant.client.converse.call_count == 1
        assert mock_message_create.call_count == 2  # User message + assistant message

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.create")
    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_search_with_tool_use(self, mock_message_create, mock_tool_use_create, assistant):
        """Test search that requires tool use."""
        mock_message = Mock()
        mock_message_create.return_value = mock_message
        mock_tool_use = Mock()
        mock_tool_use.id = "tool-use-123"
        mock_tool_use_create.return_value = mock_tool_use

        first_response = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "text": "Let me use a tool",
                            "toolUse": {"toolUseId": "tool-123", "name": "test_tool", "input": {"param": "value"}},
                        },
                    ],
                }
            },
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "tool_use",
        }

        second_response = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "text": "Now let's execute the filter",
                            "toolUse": {"toolUseId": "tool-456", "name": "execute_filter", "input": {"param": "value"}},
                        },
                    ],
                },
            },
            "usage": {"inputTokens": 15, "outputTokens": 25},
            "metrics": {"latencyMs": 150},
            "stopReason": "tool_use",
        }

        assistant.client.converse.side_effect = [first_response, second_response]

        results = list(assistant.search("test query"))

        event_types = [r["type"] for r in results]

        assert "tool_start" in event_types
        assert "tool_complete" in event_types
        assert "search_complete" in event_types

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.create")
    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_search_with_search_tool_completion(
        self, mock_message_create, mock_tool_use_create, mock_session, mock_model, mock_search_tool
    ):
        """Test search that completes with execute_filter tool."""
        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=mock_model, tools=[mock_search_tool], session=mock_session)
            assistant.client = Mock()

        mock_message = Mock()
        mock_message_create.return_value = mock_message
        mock_tool_use = Mock()
        mock_tool_use.id = "tool-use-123"
        mock_tool_use_create.return_value = mock_tool_use

        response = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "text": "Let me use a tool",
                            "toolUse": {
                                "toolUseId": "tool-123",
                                "name": "execute_filter",
                                "input": {"query": "test"},
                            },
                        },
                    ],
                }
            },
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "tool_use",
        }

        assistant.client.converse.return_value = response

        results = list(assistant.search("test query"))

        assert any(r["type"] == "search_complete" and r["result"] == "abc123" for r in results)

    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_max_tool_iterations(self, mock_message_create, assistant):
        """Test that tool iterations are limited to MAX_TOOL_ITERATIONS."""
        mock_message = Mock()
        mock_message_create.return_value = mock_message

        response = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "text": "Let me use a tool",
                            "toolUse": {"toolUseId": "tool-123", "name": "test_tool", "input": {}},
                        }
                    ],
                }
            },
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "tool_use",
        }

        assistant.client.converse.return_value = response

        with patch("usaspending_api.llm.models.db_models.ToolUse.objects.create"):
            assistant.search("test query")

        assert assistant.tool_iterations == assistant.MAX_TOOL_ITERATIONS
        assert assistant.client.converse.call_count == assistant.MAX_TOOL_ITERATIONS + 1

    def test_tool_config_property(self, assistant, mock_tool):
        """Test that tool_config is properly formatted."""
        config = assistant.tool_config

        assert "tools" in config
        assert len(config["tools"]) == 2
        assert "toolSpec" in config["tools"][0]
        assert "inputSchema" in config["tools"][0]["toolSpec"]

    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_message_ordering(self, mock_message_create, assistant):
        """Test that messages are created with correct ordering."""
        mock_message = Mock()
        mock_message_create.return_value = mock_message

        assistant.client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Response"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "end_turn",
        }

        list(assistant.search("test query"))

        calls = mock_message_create.call_args_list
        assert calls[0][1]["order"] == 0  # User message
        assert calls[1][1]["order"] == 1  # Assistant message

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.create")
    @patch("usaspending_api.llm.models.db_models.Message.objects.create")
    def test_tool_error_handling(
        self, mock_message_create, mock_tool_use_create, mock_session, mock_model, mock_search_tool
    ):
        """Test handling of tool errors."""
        with patch("boto3.client"):
            assistant = FilterSearchAssistant(model=mock_model, tools=[mock_search_tool], session=mock_session)
            assistant.client = Mock()

        mock_message = Mock()
        mock_message_create.return_value = mock_message
        mock_tool_use = Mock()
        mock_tool_use.id = "tool-use-123"
        mock_tool_use_create.return_value = mock_tool_use

        mock_search_tool.function.return_value = {"error": "Something went wrong"}

        response = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "text": "Let me use a tool",
                            "toolUse": {
                                "toolUseId": "tool-123",
                                "name": "execute_filter",
                                "input": {"query": "test"},
                            },
                        }
                    ],
                }
            },
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "tool_use",
        }

        assistant.client.converse.return_value = response

        results = list(assistant.search("test query"))

        # Should not yield search_complete when there's an error
        assert not any(r.get("type") == "search_complete" for r in results)
