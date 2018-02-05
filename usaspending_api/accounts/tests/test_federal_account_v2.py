import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def fixture_data(db):
    mommy.make('references.ToptierAgency', cgac_code='001', abbreviation='ABCD', name='Dept. of Depts')
    fa0 = mommy.make(FederalAccount, agency_identifier='001', )
    ta0 = mommy.make('accounts.TreasuryAppropriationAccount', federal_account=fa0)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier=ta0,
               obligations_incurred_total_by_tas_cpe=1000)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier=ta0,
               obligations_incurred_total_by_tas_cpe=2000)
    mommy.make('accounts.AppropriationAccountBalances',
               treasury_account_identifier__treasury_account_identifier='999',
               obligations_incurred_total_by_tas_cpe=4000)


@pytest.mark.skip
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

    resp = client.post('/api/v2/federal_accounts/', content_type='application/json', data=json.dumps({}))
    response_data = resp.json()
    assert response_data['results'][0]['budgetary_resources'] == 3000
    assert response_data['results'][0]['managing_agency'] == 'Dept. of Depts'
    assert response_data['results'][0]['managing_agency_acronym'] == 'ABCD'
