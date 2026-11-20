"""The query review and filter search run as separate sessions of one SmartAssistSearch. Its id is the
`search_id` the endpoints return, so the messages and token counts of both sessions can be found and totaled
together."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from asgiref.sync import sync_to_async
from django.utils import timezone

from usaspending_api.llm.models.db_models import AIModel, Assistant, Message, Session, SmartAssistSearch
from usaspending_api.llm.models.py_models import QueryReviewOutput
from usaspending_api.llm.session_logging import finish_session

pytestmark = pytest.mark.django_db


@pytest.fixture
def model():
    return AIModel.objects.create(name="nova pro test", model_id="amazon.nova-pro-v1:0", provider="amazon")


@pytest.fixture
def query_review_config(model):
    return Assistant.objects.create(name="query-review", ai_model=model, is_active=True, inference_config={})


@pytest.fixture
def filter_search_config(model):
    return Assistant.objects.create(name="filter-search", ai_model=model, is_active=True, inference_config={})


def _add_message(session: Session, order: int, input_tokens: int, output_tokens: int) -> None:
    Message.objects.create(
        session=session,
        role="assistant",
        message="m",
        order=order,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )


def test_query_review_starts_a_search_and_returns_its_id(client, query_review_config):
    def evaluate(self, query):
        return QueryReviewOutput(verdict="answerable", search_id=self.search_id)

    with patch("usaspending_api.llm.assistants.query_review.QueryReview.evaluate", evaluate):
        response = client.post(
            "/api/v2/llm/query-review/", content_type="application/json", data={"query": "grants in texas"}
        )

    assert response.status_code == 200
    search = SmartAssistSearch.objects.get()
    assert response.json()["search_id"] == str(search.id)
    assert search.original_query == "grants in texas"
    assert search.ended_at is not None

    session = Session.objects.get()
    assert session.search == search
    assert session.assistant == query_review_config
    assert session.ended_at is not None


@pytest.fixture(autouse=True)
def mock_guardrails():
    with patch("usaspending_api.llm.v2.views.filter_search.BedrockGuardrailService") as service:
        service.return_value.assess_input.return_value = MagicMock(intervened=False)
        yield service


def test_filter_search_joins_the_search_the_query_review_started(client, filter_search_config):
    search = SmartAssistSearch.objects.create(original_query="grants in texas")

    response = client.post(
        "/api/v2/llm/filter-search/",
        content_type="application/json",
        data={"query": "Grants to universities in Texas", "search_id": str(search.id)},
    )

    assert response.status_code == 200
    session = Session.objects.get()
    assert session.search == search
    assert session.assistant == filter_search_config
    search.refresh_from_db()
    assert search.original_query == "grants in texas"
    assert search.approved_query == "Grants to universities in Texas"
    assert SmartAssistSearch.objects.count() == 1


def test_filter_search_starts_a_search_when_there_is_none(client, filter_search_config):
    response = client.post(
        "/api/v2/llm/filter-search/", content_type="application/json", data={"query": "grants in texas"}
    )

    assert response.status_code == 200
    search = SmartAssistSearch.objects.get()
    assert search.original_query == search.approved_query == "grants in texas"
    assert Session.objects.get().search == search


def test_an_unknown_search_id_starts_a_new_search(client, filter_search_config):
    unknown = uuid.uuid4()

    response = client.post(
        "/api/v2/llm/filter-search/",
        content_type="application/json",
        data={"query": "grants in texas", "search_id": str(unknown)},
    )

    assert response.status_code == 200
    search = SmartAssistSearch.objects.get()
    assert search.id != unknown
    assert Session.objects.get().search == search


def test_search_id_must_be_a_uuid(client, filter_search_config):
    response = client.post(
        "/api/v2/llm/filter-search/",
        content_type="application/json",
        data={"query": "grants in texas", "search_id": "not-a-uuid"},
    )

    assert response.status_code == 422


def test_deleting_a_search_deletes_its_sessions_and_messages(model):
    search = SmartAssistSearch.objects.create()
    session = Session.objects.create(ai_model=model, search=search)
    _add_message(session, 0, 1, 1)

    search.delete()

    assert not Session.objects.exists()
    assert not Message.objects.exists()


@pytest.mark.django_db(transaction=True)
async def test_finish_session_logs_tokens_for_the_whole_search_and_ends_it(model):
    search = await SmartAssistSearch.objects.acreate()
    other_search = await SmartAssistSearch.objects.acreate()
    query_session = await sync_to_async(Session.objects.create)(ai_model=model, search=search)
    filter_session = await sync_to_async(Session.objects.create)(ai_model=model, search=search)
    other_session = await sync_to_async(Session.objects.create)(ai_model=model, search=other_search)
    await sync_to_async(_add_message)(query_session, 0, 100, 20)
    await sync_to_async(_add_message)(filter_session, 0, 300, 40)
    await sync_to_async(_add_message)(filter_session, 1, 50, 10)
    await sync_to_async(_add_message)(other_session, 0, 9999, 9999)  # a different search must not be counted

    with patch("usaspending_api.llm.session_logging.logger") as logger:
        await finish_session(filter_session, "Filter search")

    extra = logger.info.call_args.kwargs["extra"]
    assert extra["search_id"] == str(search.id)
    assert extra["total_tokens"] == 400
    assert extra["model_id"] == "amazon.nova-pro-v1:0"
    assert extra["message_count"] == 2
    assert extra["search_total_tokens"] == 520

    await search.arefresh_from_db()
    await filter_session.arefresh_from_db()
    assert search.ended_at == filter_session.ended_at <= timezone.now()
    await other_search.arefresh_from_db()
    assert other_search.ended_at is None


@pytest.mark.django_db(transaction=True)
async def test_finish_session_without_a_search_logs_only_its_own_tokens(model):
    session = await sync_to_async(Session.objects.create)(ai_model=model)
    await sync_to_async(_add_message)(session, 0, 7, 3)

    with patch("usaspending_api.llm.session_logging.logger") as logger:
        await finish_session(session, "Query review")

    extra = logger.info.call_args.kwargs["extra"]
    assert extra["total_tokens"] == 10
    assert extra["search_id"] is None
    assert "search_total_tokens" not in extra
