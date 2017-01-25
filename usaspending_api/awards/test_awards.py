import pytest
import json

from model_mommy import mommy

@pytest.fixture(scope="session")
def award_models():
    mommy.make('awards.FinancialAccountsByAwardsTransactionObligations', _quantity=2)

@pytest.mark.django_db
def test_award_list(award_models, client):
    """
    Ensure the awards endpoint lists the right number of awards
    """
    resp = client.get('/api/v1/awards/')
    assert resp.status_code == 200
    assert len(resp.data) >= 2

    assert client.get('/api/v1/awards/fain/ABCD').status_code == 200
    assert client.get('/api/v1/awards/uri/ABCD').status_code == 200
    assert client.get('/api/v1/awards/piid/ABCD').status_code == 200

@pytest.mark.django_db
def test_award_list_summary(award_models, client):
    """
    Ensure the awards endpoint summary lists the right number of awards
    """
    resp = client.get('/api/v1/awards/summary/')
    assert resp.status_code == 200
    assert len(resp.data) > 2

    assert client.get('/api/v1/awards/summary/fain/ABCD').status_code == 200
    assert client.get('/api/v1/awards/summary/uri/ABCD').status_code == 200
    assert client.get('/api/v1/awards/summary/piid/ABCD').status_code == 200
    assert client.get('/api/v1/awards/summary/').status_code == 200

    assert client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10})).status_code == 200
    assert client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10, "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}]})).status_code == 200
    assert client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10, "filters": [{"combine_method": "OR", "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}, {"field": "awarding_agency__toptier_agency__fpds_code", "operation": "equals", "value": "0300"}]}]})).status_code == 200
    assert client.post('/api/v1/awards/summary/', content_type='application/json', data=json.dumps({"page": 1, "limit": 10, "filters": [{"field": "funding_agency__toptier_agency__fpds_code", "operation": "ff", "value": "0300"}]})).status_code == 400
