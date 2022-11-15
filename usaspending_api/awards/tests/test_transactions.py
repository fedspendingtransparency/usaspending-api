import json

from model_bakery import baker
import pytest
from rest_framework import status


@pytest.mark.django_db
def test_transaction_endpoint_v2(client):
    """Test the transaction endpoint."""

    resp = client.post("/api/v2/transactions/", {"award_id": "1", "page": "1", "limit": "10", "order": "asc"})
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 1


@pytest.mark.django_db
def test_transaction_endpoint_v2_award_fk(client):
    """Test the transaction endpoint."""

    awd = baker.make(
        "awards.Award",
        id=10,
        total_obligation="2000",
        latest_transaction_id=1,
        earliest_transaction_id=1,
        _fill_optional=True,
        generated_unique_award_id="-TEST-",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        transaction_description="this should match",
        _fill_optional=True,
        award=awd,
    )

    resp = client.post("/api/v2/transactions/", {"award_id": 10})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"

    resp = client.post("/api/v2/transactions/", {"award_id": "-TEST-"})
    assert resp.status_code == status.HTTP_200_OK
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["description"] == "this should match"


@pytest.mark.django_db
def test_txn_total_grouped(client):
    """Test award total endpoint with a group parameter."""
    # add this test once the transaction backend is refactored
    pass
