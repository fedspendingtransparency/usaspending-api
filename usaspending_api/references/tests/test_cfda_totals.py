import pytest
import mock
from rest_framework import status
from usaspending_api.common.exceptions import NoDataFoundException, InternalServerError, ServiceUnavailable


@mock.patch(
    "requests.post",
    mock.MagicMock(return_value={"status_code": status.HTTP_503_SERVICE_UNAVAILABLE}),
)
@pytest.mark.django_db
def test_service_unavailable(client):
    # with pytest.raises(Exception) as e_info:
    print("something")
    print(client.get("/api/v2/references/cfda/totals/"))
    # print(e_info)
    # assert e_info == 0
