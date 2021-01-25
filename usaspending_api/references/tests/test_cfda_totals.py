import pytest
from rest_framework import status


@pytest.fixture
def mock_api_response(*args, **kwargs):
    class MockResponse:
        def __init__(self, status, json_data):
            self.status = status
            self.json_data = json_data

        @property
        def status_code(self):
            return self.status

        def json(self):
            return self.json_data

    print(args, kwargs)
    return MockResponse(status=args[0], json_data=args[1])


@pytest.mark.django_db
def test_api_err(client, monkeypatch, mock_api_response):
    monkeypatch.setattr(
        "requests.post", lambda *args, **kwargs: mock_api_response(status.HTTP_200_OK, {"errorMsgs": ["error msg"]})
    )
    response = client.get("/api/v2/references/cfda/totals/")
    assert (
        response.json()["detail"]
        == "Error returned by https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals: ['error msg']"
    )


# @patch(
#     "requests.post",
#     MagicMock(return_value=mock_api_response(status.HTTP_503_SERVICE_UNAVAILABLE, {})),
# )
# @patch(
#     "requests.post",
#     MagicMock(return_value=RESPONSE_MAP.pop(0)),
# )
# @pytest.mark.django_db
# def test_service_unavailable(client):
#     response = client.get("/api/v2/references/cfda/totals/")
#     assert (
#         response.json()["detail"]
#         == "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals not available (status 503)"
#     )


# # @pytest.mark.django_db
# # def test_service_unavailable(client, monkeypatch):
# #     monkeypatch.setattr(
# #         "requests.post", lambda *args, **kwargs: mock_api_response(status.HTTP_503_SERVICE_UNAVAILABLE, {})
# #     )

# #     response = client.get("/api/v2/references/cfda/totals/")
# #     assert (
# #         response.json()["detail"]
# #         == "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals not available (status 503)"
# #     )
