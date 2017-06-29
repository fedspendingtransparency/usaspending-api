import json

import pytest
from model_mommy import mommy


@pytest.fixture
def model_instances():
    mommy.make(
        'accounts.BudgetAuthority',
        year=2000,
        amount=2000000,
        agency_identifier='000')
    mommy.make(
        'accounts.BudgetAuthority',
        year=2001,
        amount=2001000,
        agency_identifier='000')
    mommy.make(
        'accounts.BudgetAuthority',
        year=2002,
        amount=2002000,
        agency_identifier='000')
    mommy.make(
        'accounts.BudgetAuthority',
        year=2000,
        amount=1000,
        agency_identifier='002')
    mommy.make(
        'accounts.BudgetAuthority',
        year=2000,
        amount=2000,
        fr_entity_code='0202',
        agency_identifier='002')


@pytest.mark.django_db
def test_budget_authority_endpoint(model_instances, client):
    resp = client.get('/api/v2/budget_authority/agencies/000/')
    assert resp.status_code == 200
    results = resp.json()['results']
    assert len(results) == 3
    for result in results:
        assert 'year' in result
        assert 'total' in result


@pytest.mark.django_db
def test_budget_authority_endpoint_no_records(model_instances, client):
    resp = client.get('/api/v2/budget_authority/agencies/001/')
    assert resp.status_code == 200
    assert not resp.json()['results']


@pytest.mark.django_db
def test_budget_authority_endpoint_no_frec_sums_all(model_instances, client):
    "If FREC is not specified, all records with that AID should be summed"
    resp = client.get('/api/v2/budget_authority/agencies/002/')
    assert resp.status_code == 200
    results = resp.json()['results']
    assert len(results) == 1
    assert results[0]['total'] == 3000


@pytest.mark.django_db
def test_budget_authority_endpoint_filters_on_frec(model_instances, client):
    "If FREC is specified, sum only records with that FREC"
    resp = client.get('/api/v2/budget_authority/agencies/002/?frec=0202')
    assert resp.status_code == 200
    results = resp.json()['results']
    assert len(results) == 1
    assert results[0]['total'] == 2000
