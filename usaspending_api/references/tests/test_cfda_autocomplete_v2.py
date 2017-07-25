import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Cfda


@pytest.fixture
def budget_function_data(db):
    mommy.make(
        Cfda,
        program_number="2222",
        program_title="test1",
        popular_name="123123123")
    mommy.make(
        Cfda,
        program_number="1111",
        program_title="test2",
        popular_name="234234234")
    mommy.make(
        Cfda,
        program_number="3333",
        program_title="test3",
        popular_name="345345345")


@pytest.mark.django_db
def test_naics_autocomplete_success(client, budget_function_data):

    # test for NAICS_description exact match
    resp = client.post(
        '/api/v2/autocomplete/cfda/',
        content_type='application/json',
        data=json.dumps({'search_text': '123123123'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['program_number'] == '2222'

    # test for similar matches (with no duplicates)
    resp = client.post(
        '/api/v2/autocomplete/cfda/',
        content_type='application/json',
        data=json.dumps({'search_text': '2223', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3
    # test closest match is at the top
    assert resp.data['results'][0]['popular_name'] == '123123123'
    assert resp.data['results'][1]['popular_name'] == '234234234'
    assert resp.data['results'][2]['popular_name'] == '345345345'


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/autocomplete/psc/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
