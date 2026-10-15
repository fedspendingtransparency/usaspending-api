import pytest

pytestmark = pytest.mark.django_db


def test_filter_search_endpoint_reachable(client):
    """
    The filter-search endpoint uses django-ninja, and we want to make sure that it is reachable
    """
    resp = client.post("/api/v2/llm/filter-search/", content_type="application/json", data={"query": "sample data"})
    assert resp.status_code == 200
