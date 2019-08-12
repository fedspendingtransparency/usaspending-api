import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_category_success(client, refresh_matviews):

    # test for required functions
    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"category": "funding_agency", "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"category": "cfda", "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK
    # test for similar matches (with no duplicates)


@pytest.mark.django_db
def test_naics_autocomplete_failure(client, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post("/api/v2/search/spending_by_category/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": all_filters()}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
