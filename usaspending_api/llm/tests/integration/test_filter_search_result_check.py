"""Tests for the steps that run after execute_filter saves filters: counting the awards, one revision when there are
none, and the "why these filters" note."""

import uuid
from itertools import count
from unittest.mock import AsyncMock, Mock, patch

import pytest

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import AIModel, Assistant, Session
from usaspending_api.llm.models.py_models import AITool, AIToolResult

COUNTS_PATH = "usaspending_api.llm.assistants.filter_search.get_award_counts"
SAVED_FILTERS_PATH = "usaspending_api.llm.assistants.filter_search.FilterSearchAssistant._saved_filter_request"
FILTER_REQUEST = {"filters": {"keyword": {"housing": "housing"}, "timePeriodType": "fy", "timePeriodFY": ["2022"]}}
NO_AWARDS = {"contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}
SOME_AWARDS = {**NO_AWARDS, "grants": 4}
EXPLANATION = "Smart Assist searched award descriptions for housing in fiscal year 2022."


def _response(content: list[dict], stop_reason: str) -> dict:
    return {
        "output": {"message": {"role": "assistant", "content": content}},
        "usage": {"inputTokens": 10, "outputTokens": 5},
        "metrics": {"latencyMs": 100},
        "stopReason": stop_reason,
    }


def execute_filter_call(tool_use_id: str) -> dict:
    content = [{"toolUse": {"toolUseId": tool_use_id, "name": "execute_filter", "input": {"keyword": ["housing"]}}}]
    return _response(content, "tool_use")


def text_reply(text: str) -> dict:
    return _response([{"text": text}], "end_turn")


@pytest.fixture
def bedrock():
    return AsyncMock()


@pytest.fixture
def explainer():
    """Stands in for FilterExplain.explain, which has its own tests."""
    return AsyncMock(return_value=EXPLANATION)


@pytest.fixture
def filter_hashes():
    """execute_filter returns a new hash each time it is called."""
    return iter(["hash-1", "hash-2", "hash-3"])


@pytest.fixture
def make_assistant(bedrock, filter_hashes, explainer):
    def _make(check_results: bool = True, explain: bool = True) -> FilterSearchAssistant:
        session = Mock(spec=Session)
        session.id = uuid.uuid4()
        session.messages = Mock()
        session.messages.aaggregate = AsyncMock(return_value={"input_tokens": 0, "output_tokens": 0})

        model = Mock(spec=AIModel)
        model.model_id = "test-model-id"
        config = Mock(spec=Assistant)
        config.ai_model = model
        config.system_prompt = Mock(text="Test system message")
        config.inference_config = {}

        search_tool = Mock(spec=AITool)
        search_tool.description = Mock()
        search_tool.description.name = "execute_filter"
        search_tool.description.model_dump = Mock(
            return_value={"name": "execute_filter", "description": "", "input_schema": {"type": "object"}}
        )
        search_tool.function = AsyncMock(side_effect=lambda **_: AIToolResult(results={"hash": next(filter_hashes)}))
        search_tool.logging = Mock(return_value="Selecting filters")

        return FilterSearchAssistant(
            assistant=config,
            tools=[search_tool],
            session=session,
            check_results=check_results,
            explainer=explainer if explain else None,
        )

    with (
        patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as session_cls,
        patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock) as message_create,
        patch(
            "usaspending_api.llm.models.db_models.ToolUse.objects.acreate", new_callable=AsyncMock
        ) as tool_use_create,
        patch(SAVED_FILTERS_PATH, new_callable=AsyncMock, return_value=FILTER_REQUEST),
    ):
        client_manager = AsyncMock()
        client_manager.__aenter__.return_value = bedrock
        session_cls.return_value.client.return_value = client_manager

        ids = count(1)
        message_create.side_effect = lambda **kwargs: Mock(id=next(ids), message=kwargs["message"])
        tool_use_create.return_value = Mock(id="tool-use", asave=AsyncMock())
        yield _make


async def run_search(assistant: FilterSearchAssistant) -> list[dict]:
    return [event async for event in assistant.search("affordable housing in New York in 2022")]


def types(events: list[dict]) -> list[str]:
    return [event["type"] for event in events]


class TestResultCheck:
    async def test_awards_found_completes_with_an_explanation_and_no_retry(self, make_assistant, bedrock, explainer):
        bedrock.converse.side_effect = [execute_filter_call("t1")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=SOME_AWARDS):
            events = await run_search(make_assistant())

        assert types(events) == [
            "search_start",
            "tool_start",
            "tool_complete",
            "search_complete",
            "search_explanation",
        ]
        assert events[3]["result"] == "hash-1"
        assert events[4]["message"] == EXPLANATION
        assert bedrock.converse.call_count == 1
        explainer.assert_awaited_once_with("affordable housing in New York in 2022", FILTER_REQUEST)

    async def test_no_awards_asks_for_one_revision_and_completes_with_the_new_filters(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1"), execute_filter_call("t2")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, side_effect=[NO_AWARDS, SOME_AWARDS]):
            events = await run_search(make_assistant())

        assert types(events) == [
            "search_start",
            "tool_start",
            "tool_complete",
            "search_retry",
            "tool_start",
            "tool_complete",
            "search_complete",
            "search_explanation",
        ]
        assert [e["result"] for e in events if e["type"] == "search_complete"] == ["hash-2"]

    async def test_the_model_is_told_the_filters_matched_nothing(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1"), text_reply("OK")]
        assistant = make_assistant()

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=NO_AWARDS):
            await run_search(assistant)

        user_turn = next(
            m
            for m in assistant.messages
            if m["role"] == "user" and any("match no awards" in b.get("text", "") for b in m["content"])
        )
        # the note rides along with the toolResult turn so that roles keep alternating
        assert any("toolResult" in block for block in user_turn["content"])

    async def test_keeps_the_original_filters_when_the_model_says_they_are_right(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1"), text_reply("OK")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=NO_AWARDS):
            events = await run_search(make_assistant())

        assert "search_retry" in types(events)
        assert [e["result"] for e in events if e["type"] == "search_complete"] == ["hash-1"]
        assert types(events)[-1] == "search_explanation"

    async def test_only_one_revision_is_allowed(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1"), execute_filter_call("t2")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=NO_AWARDS) as get_counts:
            events = await run_search(make_assistant())

        assert types(events).count("search_retry") == 1
        assert [e["result"] for e in events if e["type"] == "search_complete"] == ["hash-2"]
        assert get_counts.await_count == 2

    async def test_a_failed_count_does_not_block_the_search(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=None):
            events = await run_search(make_assistant())

        assert "search_retry" not in types(events)
        assert "search_complete" in types(events)

    async def test_a_failed_explanation_does_not_fail_the_search(self, make_assistant, bedrock, explainer):
        bedrock.converse.side_effect = [execute_filter_call("t1")]
        explainer.side_effect = RuntimeError("bedrock is down")

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=SOME_AWARDS):
            events = await run_search(make_assistant())

        assert types(events)[-1] == "search_complete"
        assert "search_explanation" not in types(events)

    async def test_no_explanation_event_when_none_is_written(self, make_assistant, bedrock, explainer):
        bedrock.converse.side_effect = [execute_filter_call("t1")]
        explainer.return_value = None

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=SOME_AWARDS):
            events = await run_search(make_assistant())

        assert types(events)[-1] == "search_complete"

    async def test_search_without_an_explainer_still_completes(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1")]

        with patch(COUNTS_PATH, new_callable=AsyncMock, return_value=SOME_AWARDS):
            events = await run_search(make_assistant(explain=False))

        assert types(events)[-1] == "search_complete"

    async def test_nothing_changes_when_the_check_is_off(self, make_assistant, bedrock):
        bedrock.converse.side_effect = [execute_filter_call("t1")]

        with patch(COUNTS_PATH, new_callable=AsyncMock) as get_counts:
            events = await run_search(make_assistant(check_results=False))

        assert types(events) == ["search_start", "tool_start", "tool_complete", "search_complete"]
        get_counts.assert_not_awaited()
        assert bedrock.converse.call_count == 1

    async def test_nothing_is_explained_when_the_check_is_off(self, make_assistant, bedrock, explainer):
        bedrock.converse.side_effect = [execute_filter_call("t1")]

        with patch(COUNTS_PATH, new_callable=AsyncMock):
            await run_search(make_assistant(check_results=False))

        explainer.assert_not_awaited()

    async def test_the_explanation_is_not_part_of_the_search_conversation(self, make_assistant, bedrock, explainer):
        bedrock.converse.side_effect = [execute_filter_call("t1"), execute_filter_call("t2")]
        assistant = make_assistant()

        with patch(COUNTS_PATH, new_callable=AsyncMock, side_effect=[NO_AWARDS, SOME_AWARDS]):
            await run_search(assistant)

        # explained once, for the final filters, after the revision
        explainer.assert_awaited_once()
        assert all(EXPLANATION not in str(m) for m in assistant.messages)
