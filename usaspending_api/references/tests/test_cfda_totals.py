import pytest
from mock import patch, MagicMock
from rest_framework import status


@patch(
    "requests.post",
    MagicMock(return_value={"cfdas": []}),
)
@pytest.mark.django_db
def test_service_unavailable(client):
    response = client.get("/api/v2/references/cfda/totals/")
    print(response.json())
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
