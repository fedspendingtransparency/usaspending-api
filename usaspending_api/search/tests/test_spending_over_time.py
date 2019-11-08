import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_over_time_success(client, refresh_matviews):

    # test for needed filters
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "fiscal_year", "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_over_time_failure(client, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_over_time/", content_type="application/json", data=json.dumps({"group": "fiscal_year"})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_spending_over_time_subawards_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": all_filters(), "subawards": True}),
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_over_time_subawards_failure(client, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        "/api/v2/search/spending_over_time",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": all_filters(), "subawards": "string"}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
