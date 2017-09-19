import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.broker.models import TransactionFPDS


@pytest.fixture
def budget_function_data(db):
    mommy.make(
        TransactionFPDS,
        product_or_service_code="12121212")
    mommy.make(
        TransactionFPDS,
        product_or_service_code="23232323")
    mommy.make(
        TransactionFPDS,
        product_or_service_code="34343434")
    mommy.make(
        TransactionFPDS,
        product_or_service_code="34343434")


@pytest.mark.django_db
def test_naics_autocomplete_success(client, budget_function_data):

    # test for NAICS_description exact match
    resp = client.post(
        '/api/v2/autocomplete/psc/',
        content_type='application/json',
        data=json.dumps({'search_text': '12121212'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['product_or_service_code'] == '12121212'

    # test for similar matches (with no duplicates)
    resp = client.post(
        '/api/v2/autocomplete/psc/',
        content_type='application/json',
        data=json.dumps({'search_text': '2323', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3
    # test closest match is at the top
    assert resp.data['results'][0]['product_or_service_code'] == '23232323'
    assert resp.data['results'][1]['product_or_service_code'] == '12121212'
    assert resp.data['results'][2]['product_or_service_code'] == '34343434'


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/autocomplete/psc/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
