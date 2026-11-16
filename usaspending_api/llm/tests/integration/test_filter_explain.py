"""FilterExplain writes the "why these filters" note as its own session of the Smart Assist search."""

from unittest.mock import AsyncMock, patch

import pytest

from usaspending_api.llm.assistants.filter_explain import ASSISTANT_NAME, FilterExplain
from usaspending_api.llm.models.db_models import AIModel, Assistant, Message, Prompts, Session, SmartAssistSearch
from usaspending_api.llm.v2.views.filter_search import _build_explainer

pytestmark = pytest.mark.django_db(transaction=True)

QUERY = "affordable housing in New York in 2022"
FILTER_REQUEST = {"filters": {"keyword": {"housing": "housing"}, "timePeriodType": "fy", "timePeriodFY": ["2022"]}}
EXPLANATION = "Smart Assist searched for housing in fiscal year 2022."


def _reply(text: str) -> dict:
    return {
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "usage": {"inputTokens": 120, "outputTokens": 30},
        "metrics": {"latencyMs": 800},
        "stopReason": "end_turn",
    }


@pytest.fixture
async def explain_assistant():
    model = await AIModel.objects.acreate(name="nova pro test", model_id="amazon.nova-pro-v1:0", provider="amazon")
    prompt = await Prompts.objects.acreate(name="filter-explain-test", description="d", text="Write the note.")
    return await Assistant.objects.select_related("ai_model", "system_prompt").aget(
        pk=(
            await Assistant.objects.acreate(
                name=ASSISTANT_NAME,
                ai_model=model,
                system_prompt=prompt,
                inference_config={"temperature": 0.0, "maxTokens": 400},
                is_active=True,
            )
        ).pk
    )


@pytest.fixture
def bedrock():
    client = AsyncMock()
    with patch("usaspending_api.common.helpers.aws_helpers.aioboto3.Session") as session_cls:
        manager = AsyncMock()
        manager.__aenter__.return_value = client
        session_cls.return_value.client.return_value = manager
        yield client


async def test_writes_the_note_and_records_its_own_session(explain_assistant, bedrock):
    bedrock.converse.return_value = _reply(f"  {EXPLANATION}  ")
    search = await SmartAssistSearch.objects.acreate(original_query=QUERY)

    explanation = await FilterExplain(explain_assistant, search).explain(QUERY, FILTER_REQUEST)

    assert explanation == EXPLANATION
    session = await Session.objects.aget(search=search)
    assert session.assistant_id == explain_assistant.pk
    assert session.ended_at is not None
    assert session.tools == []
    messages = [m async for m in Message.objects.filter(session=session).order_by("order")]
    assert [m.role for m in messages] == ["user", "assistant"]
    assert messages[0].message.startswith(f"User's search: {QUERY}")
    assert (messages[1].input_tokens, messages[1].output_tokens, messages[1].latency) == (120, 30, 800)


async def test_uses_the_prompt_and_inference_settings_of_the_assistant(explain_assistant, bedrock):
    bedrock.converse.return_value = _reply(EXPLANATION)

    await FilterExplain(explain_assistant).explain(QUERY, FILTER_REQUEST)

    call = bedrock.converse.call_args.kwargs
    assert call["modelId"] == "amazon.nova-pro-v1:0"
    assert call["system"] == [{"text": "Write the note."}]
    assert call["inferenceConfig"] == {"temperature": 0.0, "maxTokens": 400}
    assert "toolConfig" not in call


async def test_a_model_failure_returns_none_and_still_ends_the_session(explain_assistant, bedrock):
    bedrock.converse.side_effect = RuntimeError("bedrock is down")
    search = await SmartAssistSearch.objects.acreate()

    assert await FilterExplain(explain_assistant, search).explain(QUERY, FILTER_REQUEST) is None

    session = await Session.objects.aget(search=search)
    assert session.ended_at is not None


async def test_an_empty_reply_is_no_explanation(explain_assistant, bedrock):
    bedrock.converse.return_value = _reply("   ")

    assert await FilterExplain(explain_assistant).explain(QUERY, FILTER_REQUEST) is None


async def test_the_view_builds_an_explainer_from_the_active_assistant(explain_assistant):
    search = await SmartAssistSearch.objects.acreate()

    assert callable(await _build_explainer(search))


async def test_the_view_has_no_explainer_when_the_assistant_is_missing_or_inactive(explain_assistant):
    search = await SmartAssistSearch.objects.acreate()
    await Assistant.objects.filter(pk=explain_assistant.pk).aupdate(is_active=False)

    assert await _build_explainer(search) is None
