import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.django_db


@patch("usaspending_api.llm.v2.views.filter_search.BedrockGuardrailService")
def test_filter_search_endpoint_reachable(mock_service, client):
    """
    The filter-search endpoint uses django-ninja, and we want to make sure that it is reachable
    """
    instance = mock_service.return_value
    instance.assess_input.return_value = MagicMock(intervened=False)
    resp = client.post("/api/v2/llm/filter-search/", content_type="application/json", data={"query": "sample data"})
    assert resp.status_code == 200


@patch("usaspending_api.llm.v2.views.filter_search.BedrockGuardrailService")
def test_filter_search_guardrail_intervention_returns_search_error(mock_service, client):
    instance = mock_service.return_value
    instance.assess_input.return_value = MagicMock(intervened=True, action_reason="blocked", assessments=[])
    resp = client.post("/api/v2/llm/filter-search/", content_type="application/json", data={"query": "bad query"})
    assert resp.status_code == 200

    async def _collect() -> list:
        return [chunk async for chunk in resp.streaming_content]

    body = "".join(c.decode() if isinstance(c, bytes) else c for c in asyncio.run(_collect()))
    events = [json.loads(line) for line in body.splitlines()]
    assert events == [{"type": "search_error", "message": "The submitted request cannot be processed."}]
