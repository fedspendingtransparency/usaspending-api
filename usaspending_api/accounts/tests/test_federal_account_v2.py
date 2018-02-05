import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def fixture_data(db):
    mommy.make('references.ToptierAgency', cgac_code='001', abbreviation='ABCD', name='Dept. of Depts')
    mommy.make('references.ToptierAgency', cgac_code='002', abbreviation='EFGH', name='The Bureau')
    fa0 = mommy.make(FederalAccount, agency_identifier='001', )
    fa1 = mommy.make(FederalAccount, agency_identifier='002', )
    ta0 = mommy.make('accounts.TreasuryAppropriationAccount', federal_account=fa0)
    ta1 = mommy.make('accounts.TreasuryAppropriationAccount', federal_account=fa1)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier=ta0,
               obligations_incurred_total_by_tas_cpe=1000)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier=ta0,
               obligations_incurred_total_by_tas_cpe=2000)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier=ta1,
               obligations_incurred_total_by_tas_cpe=9000)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier__treasury_account_identifier='999',
               obligations_incurred_total_by_tas_cpe=4000)


@pytest.mark.django_db
def test_federal_accounts_endpoint_exists(client, fixture_data):

    resp = client.post('/api/v2/federal_accounts/', content_type='application/json', data=json.dumps({}))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_federal_accounts_endpoint_correct_form(client, fixture_data):

    resp = client.post('/api/v2/federal_accounts/', content_type='application/json', data=json.dumps({}))
    response_data = resp.json()
    assert response_data['page'] == 1
    assert 'limit' in response_data
    assert 'count' in response_data
    results = response_data['results']
    assert 'account_number' in results[0]


@pytest.mark.django_db
def test_federal_accounts_endpoint_correct_data(client, fixture_data):

    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'managing_agency',
                                                 'direction': 'asc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['budgetary_resources'] == 3000
    assert response_data['results'][0]['managing_agency'] == 'Dept. of Depts'
    assert response_data['results'][0]['managing_agency_acronym'] == 'ABCD'


@pytest.mark.django_db
def test_federal_accounts_endpoint_sorting(client, fixture_data):
    """Verify that sort parameters are applied correctly"""

    # sort by managing agency, asc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'managing_agency',
                                                 'direction': 'asc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['managing_agency'] < response_data['results'][1]['managing_agency']

    # sort by managing agency, desc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'managing_agency',
                                                 'direction': 'desc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['managing_agency'] > response_data['results'][1]['managing_agency']

    # sort by account number, asc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'account_number',
                                                 'direction': 'asc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['account_number'] < response_data['results'][1]['account_number']

    # sort by account number, desc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'account_number',
                                                 'direction': 'desc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['account_number'] > response_data['results'][1]['account_number']

    # sort by budgetary resources, asc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'budgetary_resources',
                                                 'direction': 'asc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['budgetary_resources'] < response_data['results'][1]['budgetary_resources']

    # sort by budgetary resources, desc
    resp = client.post('/api/v2/federal_accounts/',
                       content_type='application/json',
                       data=json.dumps({'sort': {'field': 'budgetary_resources',
                                                 'direction': 'desc'}}))
    response_data = resp.json()
    assert response_data['results'][0]['budgetary_resources'] > response_data['results'][1]['budgetary_resources']
