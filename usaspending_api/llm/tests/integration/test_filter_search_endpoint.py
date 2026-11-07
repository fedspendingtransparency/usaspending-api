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
