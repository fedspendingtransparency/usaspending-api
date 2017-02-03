import pytest


@pytest.mark.django_db
def test_transaction_endpoint(client):
    """Test the transaction endpoint."""

    resp = client.get('/api/v1/transactions/')
    assert resp.status_code == 200
    assert len(resp.data) > 2

    assert client.post(
        '/api/v1/transactions/?page=1&limit=4',
        content_type='application/json').status_code == 200


@pytest.mark.django_db
def test_txn_total_grouped(client):
    """Test award total endpoint with a group parameter."""
    # add this test once the transaction backend is refactored
    pass
