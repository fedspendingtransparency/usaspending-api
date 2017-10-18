import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import NAICS


@pytest.fixture
def budget_function_data(db):
    mommy.make(
        NAICS,
        code="12121212",
        description="NAICS_DESCRIPTION")
    mommy.make(
        NAICS,
        code="23232323",
        description="test1")
    mommy.make(
        NAICS,
        code="34343434",
        description="tes2")
    mommy.make(
        NAICS,
        code="34343434",
        description="tes2")


@pytest.mark.django_db
def test_naics_autocomplete_success(client, budget_function_data):

    # test for NAICS_description exact match
    resp = client.post(
        '/api/v2/autocomplete/naics/',
        content_type='application/json',
        data=json.dumps({'search_text': 'naics_description'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['naics_description'] == 'NAICS_DESCRIPTION'

    # test for similar matches (with no duplicates)
    resp = client.post(
        '/api/v2/autocomplete/naics/',
        content_type='application/json',
        data=json.dumps({'search_text': 'test', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3
    # test closest match is at the top
    assert resp.data['results'][0]['naics'] == '23232323'
    assert resp.data['results'][1]['naics'] == '34343434'
    assert resp.data['results'][2]['naics'] == '12121212'


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/autocomplete/naics/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
