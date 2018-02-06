import json
import pytest
from rest_framework import status


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_transaction_kws_success(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_transaction/',
        content_type='application/json',
        data=json.dumps(
            {
                "filters":
                    {
                        "search_term": "a",
                        "transaction_type": "Direct Payments"
                    },
                "fields": ["Award ID", "Recipient Name", "Mod"],
                "page": 1,
                "limit": 5,
                "sort": "Award ID",
                "order": "desc"
            }
        )
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_transaction_kws_failure(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_transaction/',
        content_type='application/json',
        data=json.dumps({'filters': {}}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
