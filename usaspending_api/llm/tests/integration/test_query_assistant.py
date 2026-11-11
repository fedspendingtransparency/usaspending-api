from unittest.mock import AsyncMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from usaspending_api.llm.assistants.query_assistant import (
    DEFAULT_OUT_OF_SCOPE_MESSAGE,
    SEPARATE_QUERIES_HEADING,
    SUGGESTIONS_HEADING,
    QueryAssistant,
)
from usaspending_api.llm.models.db_models import Assistant, Session
from usaspending_api.llm.models.py_models import QueryAssistantDecision


@pytest.fixture
def mock_session():
    session = Mock(spec=Session)
    session.id = "11111111-1111-1111-1111-111111111111"
    return session


@pytest.fixture
def mock_assistant():
    assistant = Mock(spec=Assistant)
    assistant.ai_model = Mock(model_id="amazon.nova-pro-v1:0")
    assistant.system_prompt = Mock(text="You are the query assistant.")
    assistant.inference_config = {"temperature": 0.0, "maxTokens": 1000, "stopSequences": []}
    return assistant


@pytest.fixture
def mock_bedrock_client():
    client = AsyncMock()
    with patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as mock_boto_session:
        mock_boto_session.return_value.client.return_value.__aenter__.return_value = client
        yield client


def _bedrock_response(decision: dict | None) -> dict:
    content = [{"text": "ok"}]
    if decision is not None:
        content = [{"toolUse": {"toolUseId": "t1", "name": QueryAssistant.TOOL_NAME, "input": decision}}]
    return {
        "output": {"message": {"role": "assistant", "content": content}},
        "usage": {"inputTokens": 10, "outputTokens": 5},
        "metrics": {"latencyMs": 100},
        "stopReason": "tool_use" if decision is not None else "end_turn",
    }


@pytest.fixture
def no_db_writes():
    with patch("usaspending_api.llm.models.db_models.Message.objects.acreate", new_callable=AsyncMock) as m:
        yield m


async def _evaluate(mock_assistant, mock_session, query="q"):
    return await QueryAssistant(mock_assistant, mock_session).evaluate(query)


@pytest.mark.asyncio
async def test_answerable(mock_assistant, mock_session, mock_bedrock_client, no_db_writes):
    mock_bedrock_client.converse.return_value = _bedrock_response({"verdict": "answerable", "reason_code": "ok"})
    output = await _evaluate(mock_assistant, mock_session, "NASA contracts")
    assert output.verdict == "answerable"
    assert output.rewritten_query is None
    assert output.suggestions == []
    assert output.search_id == mock_session.id


@pytest.mark.asyncio
async def test_rewrite(mock_assistant, mock_session, mock_bedrock_client, no_db_writes):
    mock_bedrock_client.converse.return_value = _bedrock_response(
        {
            "verdict": "answerable_after_rephrase",
            "reason_code": "no_matching_data",
            "message": "Awards are not tagged by conflict.",
            "rewritten_query": "  DoD awards with place of performance in Iran ",
            "caveat": "Not the cost of the war.",
        }
    )
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "answerable_after_rephrase"
    assert output.rewritten_query == "DoD awards with place of performance in Iran"
    assert output.caveat is None  # notes are not shown for rewrites
    assert output.suggestions == []


@pytest.mark.asyncio
async def test_comparison_is_never_a_single_rewrite(mock_session):
    """A model that rewrites a comparison into one combined query must be turned into out_of_scope."""
    decision = QueryAssistantDecision(
        verdict="answerable_after_rephrase",
        reason_code="needs_comparison",
        message="Search them separately.",
        rewritten_query="Federal funding for DC in 2025; Federal funding for Wyoming in 2025",
        separate_queries=["Federal funding for DC in 2025", "Federal funding for Wyoming in 2025"],
    )
    output = QueryAssistant.build_output(decision, mock_session)
    assert output.verdict == "out_of_scope"
    assert output.rewritten_query is None
    assert output.suggestions == ["Federal funding for DC in 2025", "Federal funding for Wyoming in 2025"]
    assert output.suggestions_heading == SEPARATE_QUERIES_HEADING


@pytest.mark.asyncio
async def test_comparison_without_separate_queries_uses_generic_examples(mock_session):
    decision = QueryAssistantDecision(
        verdict="out_of_scope", reason_code="needs_comparison", message="Cannot compare.", separate_queries=["only one"]
    )
    output = QueryAssistant.build_output(decision, mock_session)
    assert output.suggestions_heading == SUGGESTIONS_HEADING
    assert "only one" not in output.suggestions
    assert output.suggestions


@pytest.mark.asyncio
async def test_out_of_scope_gets_default_message_and_suggestions(
    mock_assistant, mock_session, mock_bedrock_client, no_db_writes
):
    mock_bedrock_client.converse.return_value = _bedrock_response(
        {"verdict": "out_of_scope", "reason_code": "not_a_search", "message": " "}
    )
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "out_of_scope"
    assert output.message == DEFAULT_OUT_OF_SCOPE_MESSAGE
    assert output.suggestions


@pytest.mark.asyncio
async def test_rewrite_without_rewritten_query_is_answerable(mock_session):
    decision = QueryAssistantDecision(
        verdict="answerable_after_rephrase", reason_code="no_matching_data", rewritten_query="  "
    )
    assert QueryAssistant.build_output(decision, mock_session).verdict == "answerable"


@pytest.mark.asyncio
async def test_no_tool_use_is_answerable(mock_assistant, mock_session, mock_bedrock_client, no_db_writes):
    mock_bedrock_client.converse.return_value = _bedrock_response(None)
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "answerable"


@pytest.mark.asyncio
async def test_throttling_is_retried(mock_assistant, mock_session, mock_bedrock_client, no_db_writes, monkeypatch):
    async def no_sleep(_):
        return None

    monkeypatch.setattr("asyncio.sleep", no_sleep)
    throttle = ClientError({"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "Converse")
    mock_bedrock_client.converse.side_effect = [
        throttle,
        _bedrock_response({"verdict": "answerable", "reason_code": "ok"}),
    ]
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "answerable"
    assert mock_bedrock_client.converse.call_count == 2
