import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.mark.django_db
def test_spending_by_category_success(client, monkeypatch, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

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
        data=json.dumps({"category": "cfda", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK
    # test for similar matches (with no duplicates)


@pytest.mark.django_db
def test_naics_autocomplete_failure(client, monkeypatch, elasticsearch_transaction_index):
    """Verify error on bad autocomplete request for budget function."""
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post("/api/v2/search/spending_by_category", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    resp = client.post(
        "/api/v2/search/spending_by_category",
        content_type="application/json",
        data=json.dumps({"group": "quarter", "filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
