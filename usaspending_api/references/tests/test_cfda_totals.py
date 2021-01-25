import pytest
from rest_framework import status


def mock_api_response(monkeypatch, status, json_data):
    class MockResponse:
        def __init__(self, status, json_data):
            self.status = status
            self.json_data = json_data

        @property
        def status_code(self):
            return self.status

        def json(self):
            return self.json_data

    monkeypatch.setattr(
        "usaspending_api.references.v2.views.cfda.post", lambda *args, **kwargs: MockResponse(status, json_data)
    )


@pytest.mark.django_db
def test_api_err(client, monkeypatch):
    mock_api_response(monkeypatch=monkeypatch, status=status.HTTP_200_OK, json_data={"errorMsgs": ["error msg"]})
    response = client.get("/api/v2/references/cfda/totals/")
    assert (
        response.json()["detail"]
        == "Error returned by https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals: ['error msg']"
    )


@pytest.mark.django_db
def test_service_unavailable(client, monkeypatch):
    mock_api_response(monkeypatch=monkeypatch, status=status.HTTP_503_SERVICE_UNAVAILABLE, json_data={})
    response = client.get("/api/v2/references/cfda/totals/")
    assert (
        response.json()["detail"]
        == "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals not available (status 503)"
    )

