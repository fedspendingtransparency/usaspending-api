import pytest
import json

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award


@pytest.mark.django_db
def test_award_endpoint(client):
    """Test the awards endpoint."""

    resp = client.get('/api/v1/awards/')
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data) > 2

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json').status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "equals",
                "value": "0300"
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "combine_method": "OR",
                "filters": [{
                    "field": "funding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }, {
                    "field": "awarding_agency__toptier_agency__fpds_code",
                    "operation": "equals",
                    "value": "0300"
                }]
            }]
        })).status_code == status.HTTP_200_OK

    assert client.post(
        '/api/v1/awards/?page=1&limit=10',
        content_type='application/json',
        data=json.dumps({
            "filters": [{
                "field": "funding_agency__toptier_agency__fpds_code",
                "operation": "ff",
                "value": "0300"
            }]
        })).status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_null_awards():
    """Test the award.nonempty command."""
    mommy.make('awards.Award', total_obligation="2000", _quantity=2)
    mommy.make(
        'awards.Award',
        type="U",
        total_obligation=None,
        date_signed=None,
        recipient=None)

    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2


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


def test_award_total_grouped(client, awards_data):
    """Test award total endpoint with a group parameter."""

    resp = client.post(
        '/api/v1/awards/total/',
        content_type='application/json',
        data=json.dumps({
            'field': 'total_obligation',
            'group': 'type',
            'aggregate': 'sum'
        }))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.data['results']
    # our data has two different type codes, we should get two summarized items back
    assert len(results) == 2
    # check total
    for result in resp.data['results']:
        if result['item'] == 'B':
            assert float(result['aggregate']) == 2000
        else:
            assert float(result['aggregate']) == 1000


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
def test_award_autocomplete(client, awards_data, fields, value, expected):
    """Test partial-text search."""

    for match_objs in (0, 1):
        resp = client.post(
            '/api/v1/awards/autocomplete/',
            content_type='application/json',
            data=json.dumps({
                'fields': fields,
                'value': value,
                'matched_objects': match_objs
            }))
        assert resp.status_code == status.HTTP_200_OK

        results = resp.data['results']
        for key in results:
            sorted(results[key]) == expected[key]

        # If matched_objects requested, verify they are present
        assert ('matched_objects' in resp.data) == match_objs
        if match_objs:
            objs = resp.data['matched_objects']
            for field in fields:
                assert field in objs
                # and return correct number of objects
                assert len(objs[field]) == len(results[field])


def test_bad_autocomplete_request(client):
    """Verify error on bad autocomplete request."""

    resp = client.post(
        '/api/v1/awards/autocomplete/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
