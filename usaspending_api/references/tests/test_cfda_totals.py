import pytest
from unittest.mock import patch, MagicMock
from rest_framework import status

# from rest_framework.response import Response
# from django.http import JsonResponse


def mock_api_response(*args, **kwargs):
    class MockResponse:
        def __init__(self, status, json_data):
            self.status = status
            self.json_data = json_data

        def status_code(self):
            return self.status

        def json(self):
            return self.json_data

    return MockResponse(args[0], args[1])


@patch(
    "usaspending_api.references.v2.views.cfda.requests.post",
    MagicMock(return_value=mock_api_response(200, {"cfdas": [], "errorMsgs": []})),
)
@pytest.mark.django_db
def test_no_data(client):
    response = client.get("/api/v2/references/cfda/totals/")
    print("response.json()['detail']")
    print(response.json()["detail"])
    assert (
        response.json()["detail"]
        == "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals not available (status 503)"
    )


# @patch(
#     "requests.post",
#     MagicMock(return_value={"status_code": status.HTTP_503_SERVICE_UNAVAILABLE}),
# )
# @pytest.mark.django_db
# def test_service_unavailable(client):
#     response = client.get("/api/v2/references/cfda/totals/")
#     assert (
#         response.json()["detail"]
#         == "https://www.grants.gov/grantsws/rest/opportunities/search/cfda/totals not available (status 503)"
#     )
