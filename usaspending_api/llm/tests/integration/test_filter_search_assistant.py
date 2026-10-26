import uuid
from unittest.mock import AsyncMock, Mock, patch

import pytest
from asgiref.sync import sync_to_async
from model_bakery import baker

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import AIModel, Assistant, Session
from usaspending_api.llm.models.py_models import AITool, clear_defc_rows_cache
from usaspending_api.llm.tools.exceptions import DefcCodeNotRecognizedError
from usaspending_api.llm.tools.execute_filter import execute_filter_tool


@pytest.fixture
def mock_session():
    session = Mock(spec=Session)
    session.id = uuid.uuid4()

    # Track messages created during the test.
    session._test_messages = []

    mock_messages_manager = Mock()
    mock_messages_manager.count.return_value = 0

    async def aaggregate(*args, **kwargs):
        input_tokens = sum(getattr(msg, "input_tokens", 0) or 0 for msg in session._test_messages)
        output_tokens = sum(getattr(msg, "output_tokens", 0) or 0 for msg in session._test_messages)
        return {"input_tokens": input_tokens, "output_tokens": output_tokens}

    mock_messages_manager.aaggregate = AsyncMock(side_effect=aaggregate)
    session.messages = mock_messages_manager

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
    tool.function = AsyncMock(return_value={"result": "success"})
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
    tool.function = AsyncMock(return_value={"hash": "abc123", "results": []})
    tool.logging = Mock(return_value="Searching federal contracts")
    return tool


@pytest.fixture
def mock_assistant(mock_model):
    assistant = Mock(spec=Assistant)
    assistant.ai_model = mock_model
    assistant.system_prompt = Mock(text="Test system message")
    assistant.inference_config = {}
    return assistant


@pytest.fixture
def mock_bedrock_client():
    """AsyncMock standing in for the aioboto3 bedrock-runtime client."""
    return AsyncMock()


@pytest.fixture
def assistant(mock_assistant, mock_tool, mock_search_tool, mock_session, mock_bedrock_client):
    with patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as mock_session_cls:
        cm = AsyncMock()
        cm.__aenter__.return_value = mock_bedrock_client
        mock_session_cls.return_value.client.return_value = cm

        assistant = FilterSearchAssistant(
            assistant=mock_assistant,
            tools=[mock_tool, mock_search_tool],
            session=mock_session,
        )
        assistant.client = mock_bedrock_client
        yield assistant


class TestFilterSearchAssistant:
    def test_empty_inference_config_uses_deterministic_defaults(self, assistant):
        assert assistant.inference_config == {
            "temperature": 0.0,
            "topP": 1.0,
            "maxTokens": 5000,
            "stopSequences": [],
        }

    def test_null_inference_values_are_omitted(self, mock_assistant, mock_tool, mock_session):
        mock_assistant.inference_config = {"temperature": 0.4, "topP": None, "maxTokens": None}
        assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[mock_tool], session=mock_session)

        assert assistant.inference_config == {"temperature": 0.4}

    def test_all_null_inference_values_produce_empty_config(self, mock_assistant, mock_tool, mock_session):
        mock_assistant.inference_config = {"temperature": None, "topP": None, "maxTokens": None}
        assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[mock_tool], session=mock_session)

        assert assistant.inference_config == {}

    def test_system_message_uses_assistant_prompt(self, assistant):
        assert assistant.system_message == "Test system message" + FilterSearchAssistant._fiscal_year_date_context()

    def test_system_message_uses_default_when_assistant_has_no_prompt(self, mock_assistant, mock_tool, mock_session):
        mock_assistant.system_prompt = None
        assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[mock_tool], session=mock_session)

        assert assistant.system_message == FilterSearchAssistant.DEFAULT_SYSTEM_MESSAGE

    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_search_simple_response(self, mock_message_create, assistant):
        """A simple text response (no tool use) never calls execute_filter, so the search
        is considered incomplete and raises -- this assistant's only valid outcome is a filter."""

        def create_message(**kwargs):
            mock_message = Mock()
            mock_message.input_tokens = kwargs.get("input_tokens", 10)
            mock_message.output_tokens = kwargs.get("output_tokens", 20)
            mock_message.id = len(assistant.session._test_messages) + 1
            mock_message.tool_uses = Mock()
            mock_message.tool_uses.count.return_value = 0
            assistant.session._test_messages.append(mock_message)
            return mock_message

        mock_message_create.side_effect = create_message

        assistant.client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Here's your answer"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "end_turn",
        }

        results = []
        with pytest.raises(RuntimeError, match="ended without completing"):
            async for event in assistant.search("test query"):
                results.append(event)

        assert len(results) == 1
        assert results[0]["type"] == "search_start"
        assert assistant.client.converse.call_count == 1
        assert mock_message_create.call_count == 2  # User message + assistant message

    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_assistant_message_text_is_persisted(self, mock_message_create, assistant):
        """Regression test: the assistant's response text must be saved to Message.message.

        Previously `_create_message_from_response` passed the whole `{role, content}` message
        dict into `_extract_text_from_content` instead of its `content` list. Iterating a dict
        yields its string keys ("role", "content"), none of which contain a "text" key, so the
        extracted text was silently always "".
        """

        def create_message(**kwargs):
            mock_message = Mock()
            mock_message.input_tokens = kwargs.get("input_tokens", 10)
            mock_message.output_tokens = kwargs.get("output_tokens", 20)
            mock_message.id = len(assistant.session._test_messages) + 1
            mock_message.tool_uses = Mock()
            mock_message.tool_uses.count.return_value = 0
            assistant.session._test_messages.append(mock_message)
            return mock_message

        mock_message_create.side_effect = create_message

        assistant.client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Here's your answer"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "end_turn",
        }

        with pytest.raises(RuntimeError, match="ended without completing"):
            _ = [event async for event in assistant.search("test query")]

        assistant_message_call = mock_message_create.call_args_list[1]
        assert assistant_message_call.kwargs["message"] == "Here's your answer"

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock)
    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_search_with_tool_use(self, mock_message_create, mock_tool_use_create, assistant):
        """Test search that requires tool use."""

        def create_message(**kwargs):
            mock_message = Mock()
            mock_message.input_tokens = kwargs.get("input_tokens", 10)
            mock_message.output_tokens = kwargs.get("output_tokens", 20)
            mock_message.id = len(assistant.session._test_messages) + 1
            mock_message.tool_uses = Mock()
            mock_message.tool_uses.count.return_value = 0
            assistant.session._test_messages.append(mock_message)
            return mock_message

        mock_message_create.side_effect = create_message

        mock_tool_use = Mock()
        mock_tool_use.id = "tool-use-123"
        mock_tool_use.asave = AsyncMock()
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

        results = [event async for event in assistant.search("test query")]

        event_types = [r["type"] for r in results]

        assert "tool_start" in event_types
        assert "tool_complete" in event_types
        assert "search_complete" in event_types

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock)
    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_search_with_search_tool_completion(
        self,
        mock_message_create,
        mock_tool_use_create,
        mock_session,
        mock_assistant,
        mock_search_tool,
        mock_bedrock_client,
    ):
        """Test search that completes with execute_filter tool."""
        with patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as mock_session_cls:
            cm = AsyncMock()
            cm.__aenter__.return_value = mock_bedrock_client
            mock_session_cls.return_value.client.return_value = cm

            assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[mock_search_tool], session=mock_session)
            assistant.client = mock_bedrock_client

            def create_message(**kwargs):
                mock_message = Mock()
                mock_message.input_tokens = kwargs.get("input_tokens", 10)
                mock_message.output_tokens = kwargs.get("output_tokens", 20)
                mock_message.id = len(assistant.session._test_messages) + 1
                mock_message.tool_uses = Mock()
                mock_message.tool_uses.count.return_value = 0
                assistant.session._test_messages.append(mock_message)
                return mock_message

            mock_message_create.side_effect = create_message

            mock_tool_use = Mock()
            mock_tool_use.id = "tool-use-123"
            mock_tool_use.asave = AsyncMock()
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

            results = [event async for event in assistant.search("test query")]

        assert any(r["type"] == "search_complete" and r["result"] == "abc123" for r in results)

    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_max_tool_iterations(self, mock_message_create, assistant):
        """Test that tool iterations are limited to MAX_TOOL_ITERATIONS."""

        def create_message(**kwargs):
            mock_message = Mock()
            mock_message.input_tokens = kwargs.get("input_tokens", 10)
            mock_message.output_tokens = kwargs.get("output_tokens", 20)
            mock_message.id = len(assistant.session._test_messages) + 1
            mock_message.tool_uses = Mock()
            mock_message.tool_uses.count.return_value = 0
            assistant.session._test_messages.append(mock_message)
            return mock_message

        mock_message_create.side_effect = create_message

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

        with patch(
            "usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock
        ) as mock_tool_use_create:
            mock_tool_use_create.return_value.asave = AsyncMock()
            with pytest.raises(RuntimeError, match="ended without completing"):
                _ = [event async for event in assistant.search("test query")]

        assert assistant.tool_iterations == assistant.MAX_TOOL_ITERATIONS
        assert assistant.client.converse.call_count == assistant.MAX_TOOL_ITERATIONS + 1

    async def test_tool_config_property(self, assistant, mock_tool):
        """Test that tool_config is properly formatted."""
        config = await assistant.aget_tool_config()

        assert "tools" in config
        assert len(config["tools"]) == 2
        assert "toolSpec" in config["tools"][0]
        assert "inputSchema" in config["tools"][0]["toolSpec"]

    async def test_tool_config_defaults_to_any_tool_choice(self, assistant):
        """Force the model to call one of the configured tools."""
        config = await assistant.aget_tool_config()

        assert config["toolChoice"] == {"any": {}}

    async def test_tool_config_forces_completion_tool_on_final_iteration(self, assistant):
        """On the last tool-use iteration, execute_filter is forced so the search always completes."""
        assistant.tool_iterations = assistant.MAX_TOOL_ITERATIONS - 1
        config = await assistant.aget_tool_config()

        assert config["toolChoice"] == {"tool": {"name": assistant.COMPLETION_TOOL_NAME}}

    @pytest.mark.django_db
    async def test_tool_config_patches_defcodes_description_from_db(self, mock_assistant, mock_session):
        """tool_config injects a DB-sourced defCodes description into execute_filter's schema.

        The description is patched lazily here (not at execute_filter_tool's module-import time)
        so the DB query only ever runs once real DB access is available -- see get_defc_rows/aget_defc_rows.
        """
        clear_defc_rows_cache()
        await sync_to_async(baker.make)(
            "references.DisasterEmergencyFundCode",
            code="L",
            public_law="PUBLIC LAW FOR CODE L",
            title="TITLE FOR CODE L",
            group_name="covid_19",
        )

        assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[execute_filter_tool], session=mock_session)
        config = await assistant.aget_tool_config()
        description = config["tools"][0]["toolSpec"]["inputSchema"]["json"]["properties"]["defCodes"]["description"]

        assert "TITLE FOR CODE L" in description

        clear_defc_rows_cache()

    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_message_ordering(self, mock_message_create, assistant):
        """Test that messages are created with correct ordering."""

        def create_message(**kwargs):
            mock_message = Mock()
            mock_message.input_tokens = kwargs.get("input_tokens", 10)
            mock_message.output_tokens = kwargs.get("output_tokens", 20)
            mock_message.id = len(assistant.session._test_messages) + 1
            mock_message.tool_uses = Mock()
            mock_message.tool_uses.count.return_value = 0
            assistant.session._test_messages.append(mock_message)
            return mock_message

        mock_message_create.side_effect = create_message

        assistant.client.converse.return_value = {
            "output": {"message": {"role": "assistant", "content": [{"text": "Response"}]}},
            "usage": {"inputTokens": 10, "outputTokens": 20},
            "metrics": {"latencyMs": 100},
            "stopReason": "end_turn",
        }

        with pytest.raises(RuntimeError, match="ended without completing"):
            _ = [event async for event in assistant.search("test query")]

        calls = mock_message_create.call_args_list
        assert calls[0][1]["order"] == 0  # User message
        assert calls[1][1]["order"] == 1  # Assistant message

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock)
    @patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock)
    async def test_tool_error_handling(
        self,
        mock_message_create,
        mock_tool_use_create,
        mock_session,
        mock_assistant,
        mock_search_tool,
        mock_bedrock_client,
    ):
        """Test handling of tool errors."""
        with patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as mock_session_cls:
            cm = AsyncMock()
            cm.__aenter__.return_value = mock_bedrock_client
            mock_session_cls.return_value.client.return_value = cm

            assistant = FilterSearchAssistant(assistant=mock_assistant, tools=[mock_search_tool], session=mock_session)
            assistant.client = mock_bedrock_client

            def create_message(**kwargs):
                mock_message = Mock()
                mock_message.input_tokens = kwargs.get("input_tokens", 10)
                mock_message.output_tokens = kwargs.get("output_tokens", 20)
                mock_message.id = len(assistant.session._test_messages) + 1
                mock_message.tool_uses = Mock()
                mock_message.tool_uses.count.return_value = 0
                assistant.session._test_messages.append(mock_message)
                return mock_message

            mock_message_create.side_effect = create_message

            mock_tool_use = Mock()
            mock_tool_use.id = "tool-use-123"
            mock_tool_use.asave = AsyncMock()
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

            results = []
            with pytest.raises(RuntimeError, match="ended without completing"):
                async for event in assistant.search("test query"):
                    results.append(event)

        # Should not yield search_complete when there's an error
        assert not any(r.get("type") == "search_complete" for r in results)

    @patch("usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock)
    async def test_handle_tool_use_splits_user_and_model_messages(
        self, mock_tool_use_create, assistant, mock_search_tool
    ):
        """A ToolUseRuntimeError's user_message goes to the tool_error event shown to the user;
        model_detail (the richer pydantic guidance) goes to the toolResult content fed back to the
        model so it can self-correct."""
        mock_tool_use = Mock()
        mock_tool_use.id = "tool-use-456"
        mock_tool_use.asave = AsyncMock()
        mock_tool_use_create.return_value = mock_tool_use

        mock_search_tool.function.side_effect = DefcCodeNotRecognizedError(
            "DEFC code not found: BAD.", model_detail="Value error, Invalid DEFC code(s): ['BAD']."
        )

        tool_requests = [
            {
                "toolUse": {
                    "toolUseId": "tool-456",
                    "name": "execute_filter",
                    "input": {"defCodes": {"require": ["BAD"]}},
                }
            }
        ]
        mock_message = Mock()

        events = [event async for event in assistant.handle_tool_use(tool_requests, mock_message)]

        tool_error_events = [e for e in events if e["type"] == "tool_error"]
        assert len(tool_error_events) == 1
        assert tool_error_events[0]["message"] == "DEFC code not found: BAD."

        tool_result_message = assistant.messages[-1]
        tool_result_content = tool_result_message["content"][0]["toolResult"]["content"][0]["json"]
        assert tool_result_content["error"] == "Value error, Invalid DEFC code(s): ['BAD']."
