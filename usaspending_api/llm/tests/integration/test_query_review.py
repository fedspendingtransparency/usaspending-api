from unittest.mock import AsyncMock, Mock, patch

import pytest
from botocore.exceptions import ClientError

from usaspending_api.llm.assistants.query_review import (
    DEFAULT_OUT_OF_SCOPE_MESSAGE,
    QueryReview,
)
from usaspending_api.llm.models.db_models import Assistant, Session
from usaspending_api.llm.models.py_models import QueryReviewDecision

SEARCH_ID = "22222222-2222-2222-2222-222222222222"


@pytest.fixture
def mock_session():
    session = Mock(spec=Session)
    session.id = "11111111-1111-1111-1111-111111111111"
    session.search_id = "22222222-2222-2222-2222-222222222222"
    return session


@pytest.fixture
def mock_assistant():
    assistant = Mock(spec=Assistant)
    assistant.ai_model = Mock(model_id="amazon.nova-pro-v1:0")
    assistant.system_prompt = Mock(text="You are the query review.")
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
        content = [{"toolUse": {"toolUseId": "t1", "name": QueryReview.TOOL_NAME, "input": decision}}]
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
    return await QueryReview(mock_assistant, mock_session).evaluate(query)


@pytest.mark.asyncio
async def test_answerable(mock_assistant, mock_session, mock_bedrock_client, no_db_writes):
    mock_bedrock_client.converse.return_value = _bedrock_response({"verdict": "answerable"})
    output = await _evaluate(mock_assistant, mock_session, "NASA contracts")
    assert output.verdict == "answerable"
    assert output.rewritten_query is None
    assert output.search_id == mock_session.search_id


@pytest.mark.asyncio
async def test_rewrite(mock_assistant, mock_session, mock_bedrock_client, no_db_writes):
    mock_bedrock_client.converse.return_value = _bedrock_response(
        {
            "verdict": "answerable_after_rephrase",
            "message": "Reworded your question as a search for matching awards.",
            "rewritten_query": "  Awards to Arizona in 2025 ",
        }
    )
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "answerable_after_rephrase"
    assert output.rewritten_query == "Awards to Arizona in 2025"
    assert output.message == "Reworded your question as a search for matching awards."


@pytest.mark.asyncio
async def test_out_of_scope_gets_default_message(
    mock_assistant, mock_session, mock_bedrock_client, no_db_writes
):
    mock_bedrock_client.converse.return_value = _bedrock_response(
        {"verdict": "out_of_scope", "message": " "}
    )
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "out_of_scope"
    assert output.message == DEFAULT_OUT_OF_SCOPE_MESSAGE


@pytest.mark.asyncio
async def test_rewrite_without_rewritten_query_is_answerable():
    decision = QueryReviewDecision(verdict="answerable_after_rephrase", rewritten_query="  ")
    assert QueryReview.build_output(decision, SEARCH_ID).verdict == "answerable"


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
        _bedrock_response({"verdict": "answerable"}),
    ]
    output = await _evaluate(mock_assistant, mock_session)
    assert output.verdict == "answerable"
    assert mock_bedrock_client.converse.call_count == 2
