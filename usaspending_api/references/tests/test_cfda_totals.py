import pytest
from mock import patch, Mock, MagicMock
from rest_framework import status
from rest_framework.response import Response
from django.http import JsonResponse


@patch(
    "requests.post",
    MagicMock(return_value=Mock(status_code=status.HTTP_200_OK, json={"cfdas": []})),
)
@pytest.mark.django_db
def test_service_unavailable(client):
    response = client.get("/api/v2/references/cfda/totals/")
    print(response)
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
