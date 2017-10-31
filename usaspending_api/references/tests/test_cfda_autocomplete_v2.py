import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Cfda


@pytest.fixture
def cfda_data(db):
    mommy.make(
        Cfda,
        program_number="10.4837",
        program_title="test1",
        popular_name="abc123")
    mommy.make(
        Cfda,
        program_number="10.4838",
        program_title="test2",
        popular_name="abc234")
    mommy.make(
        Cfda,
        program_number="10.4839",
        program_title="test3",
        popular_name="xyz345")


@pytest.mark.django_db
def test_cfda_autocomplete_success(client, cfda_data):

    # test for program number exact match
    resp = client.post(
        '/api/v2/autocomplete/cfda/',
        content_type='application/json',
        data=json.dumps({'search_text': '10.4839'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['program_title'] == 'test3'

    # test for similar matches
    resp = client.post(
        '/api/v2/autocomplete/cfda/',
        content_type='application/json',
        data=json.dumps({'search_text': 'abc', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/autocomplete/psc/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
