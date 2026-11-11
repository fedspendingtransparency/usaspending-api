from unittest.mock import patch

import pytest

from usaspending_api.llm.models.py_models import QueryAssistantOutput

pytestmark = pytest.mark.django_db

URL = "/api/v2/llm/query-assistant/"


def test_query_assistant_fails_open_without_active_assistant(client):
    """No active query-assistant row: the query should be reported as answerable so filter search can still run."""
    resp = client.post(URL, content_type="application/json", data={"query": "sample data"})
    assert resp.status_code == 200
    assert resp.json()["verdict"] == "answerable"


def test_query_assistant_fails_open_on_error(client):
    from usaspending_api.llm.models.db_models import AIModel, Assistant

    model = AIModel.objects.create(name="nova pro test", model_id="amazon.nova-pro-v1:0", provider="amazon")
    Assistant.objects.create(name="query-assistant", ai_model=model, is_active=True, inference_config={})
    with patch(
        "usaspending_api.llm.v2.views.query_assistant.QueryAssistant.evaluate", side_effect=RuntimeError("boom")
    ):
        resp = client.post(URL, content_type="application/json", data={"query": "sample data"})
    assert resp.status_code == 200
    assert resp.json()["verdict"] == "answerable"


def test_query_assistant_returns_decision(client):
    from usaspending_api.llm.models.db_models import AIModel, Assistant

    model = AIModel.objects.create(name="nova pro test", model_id="amazon.nova-pro-v1:0", provider="amazon")
    Assistant.objects.create(name="query-assistant", ai_model=model, is_active=True, inference_config={})
    output = QueryAssistantOutput(
        verdict="out_of_scope", reason_code="no_matching_data", message="No wealth data.", suggestions=["a"]
    )
    with patch("usaspending_api.llm.v2.views.query_assistant.QueryAssistant.evaluate", return_value=output):
        resp = client.post(URL, content_type="application/json", data={"query": "billionaires"})
    assert resp.status_code == 200
    assert resp.json()["verdict"] == "out_of_scope"
    assert resp.json()["suggestions"] == ["a"]


@pytest.mark.parametrize("query", ["", "x" * 1001])
def test_query_assistant_validates_query_length(client, query):
    resp = client.post(URL, content_type="application/json", data={"query": query})
    assert resp.status_code == 422
