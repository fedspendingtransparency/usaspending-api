from rest_framework_tracking.models import APIRequestLog
from rest_framework import status

import pytest


@pytest.mark.skip(reason="Table logging is now disabled")
@pytest.mark.django_db()
def test_drf_tracking_logging(client):
    # Hit an endpoint
    endpoint_ping = client.get("/api/v1/awards/?page=1&limit=10")
    assert endpoint_ping.status_code == status.HTTP_200_OK

    # Check that we have a APIRequestLog
    assert APIRequestLog.objects.count() == 1

    # Check that the path is our path
    assert APIRequestLog.objects.first().path == "/api/v1/awards/"

    # Check that our query params are right
    queryparams = eval(APIRequestLog.objects.first().query_params)
    assert queryparams["page"] == "1"
    assert queryparams["limit"] == "10"
