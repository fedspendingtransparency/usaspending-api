import pytest

from model_mommy import mommy
import json


@pytest.fixture
def model_instances():
    mommy.make('accounts.BudgetAuthority', year=2000, amount=2000000, agency_identifier='000')
    mommy.make('accounts.BudgetAuthority', year=2001, amount=2001000, agency_identifier='000')
    mommy.make('accounts.BudgetAuthority', year=2002, amount=2002000, agency_identifier='000')


@pytest.mark.django_db
def test_budget_authority_endpoint(model_instances, client):
    resp = client.get('/api/v2/budget_authority/agencies/000/')
    assert resp.status_code == 200
    results = resp.json()['results']
    assert len(results) == 3
    for result in results:
        assert 'year' in result
        assert 'amount' in result


@pytest.mark.django_db
def test_budget_authority_endpoint_no_records(model_instances, client):
    resp = client.get('/api/v2/budget_authority/agencies/001/')
    assert resp.status_code == 200
    assert not resp.json()['results']
