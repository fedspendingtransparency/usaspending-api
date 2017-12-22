import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.common.tests.autocomplete import check_autocomplete


@pytest.fixture
def awards_data(db):
    mommy.make(
        'awards.Award',
        piid='zzz',
        fain='abc123',
        type='B',
        total_obligation=1000)
    mommy.make(
        'awards.Award',
        piid='###',
        fain='ABC789',
        type='B',
        total_obligation=1000)
    mommy.make('awards.Award', fain='XYZ789', type='C', total_obligation=1000)


@pytest.mark.parametrize("fields,value,expected", [
    (['fain', 'piid'], 'z', {
        'fain': ['XYZ789'],
        'piid': ['zzz']
    }),
    (['fain'], 'ab', {
        'fain': ['abc123', 'ABC789']
    }),
    (['fain'], '12', {
        'fain': ['abc123']
    }),
    (['fain'], '789', {
        'fain': ['XYZ789', 'ABC789']
    }),
    (['piid'], '###', {
        'piid': '###'
    }),
    (['piid'], 'ghi', {
        'piid': []
    }),
])
@pytest.mark.django_db
def test_awards_autocomplete(client, awards_data, fields, value, expected):
    """test partial-text search on awards."""

    check_autocomplete('awards', client, fields, value, expected)


@pytest.mark.django_db
def test_bad_awards_autocomplete_request(client):
    """Verify error on bad autocomplete request for awards."""

    resp = client.post(
        '/api/v1/awards/autocomplete/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
