import json
import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_transaction_count/"


@pytest.fixture
def transaction_data(db):
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_description="DEV-10812",
        recipient_location_zip5="abcde",
        piid="IND12PB00323",
        recipient_uei="testuei",
        parent_uei="test_parent_uei",
    )
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1, is_fpds=True, type="A", piid="IND12PB00323")


@pytest.mark.django_db
def test_spending_by_transaction_count_success(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["DEV-10812"], "award_type_codes": ["A"]}}),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) > 0
    assert resp_results["contracts"] == 1

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["DEV-10812"], "award_type_codes": ["B"]}}),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) > 0
    assert resp_results["contracts"] == 0

    # Test that `keyword` can be used
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps({"filters": {"keyword": "DEV-10812", "award_type_codes": ["A"]}}),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) > 0
    assert resp_results["contracts"] == 1

    # Test that `keyword` or `keywords` is not required
    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps({"filters": {"award_type_codes": ["A"]}}),
    )

    resp_results = resp.data.get("results", {})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp_results) > 0
    assert resp_results["contracts"] == 1
