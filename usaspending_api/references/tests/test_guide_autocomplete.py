import json

import pytest
from model_mommy import mommy
from rest_framework import status
from django.core.management import call_command


from usaspending_api.references.models import Definition
from usaspending_api.common.tests.autocomplete import check_autocomplete


@pytest.fixture()
def guide_data(db):
    call_command('load_guide')

# @pytest.fixture
# def guide_data(db):
#     mommy.make(
#         Definition,
#         term='Word',
#         plain='Plaintext response.',
#         official='Official language.')
#     mommy.make(
#         Definition,
#         term='Word2',
#         plain='Plaintext response. 2',
#         official='Official language. 2')
#     mommy.make(
#         Definition,
#         term='Word3',
#         plain='Plaintext response. 3',
#         official='Official language. 3')
#
#
# @pytest.mark.parametrize("fields,value,expected", [
#     (['term'], 'Word2', {
#         'plain': 'Plaintext response. 2',
#         'official': 'Official language. 2'
#     }),
# ])
#
# @pytest.mark.django_db
# def test_guides_autocomplete(client, guide_data, fields, value, expected):
#     """test partial-text search on awards."""
#
#     check_autocomplete('guide', client, fields, value, expected)
#
#
# @pytest.mark.django_db
# def test_bad_guides_autocomplete_request(client):
#     """Verify error on bad autocomplete request for awards."""
#
#     resp = client.post(
#         '/api/v1/references/guide/autocomplete/',
#         content_type='application/json',
#         data=json.dumps({}))
#     assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_guide_autocomplete_endpoint(client, guide_data):

    # Sorry to squash these together, but I don't want to load the guide data
    # multiple times

    resp = client.get('/api/v1/references/guide/autocomplete/')
    assert resp.status_code == 200
    assert len(resp.data["results"]) > 70

    resp = client.get(
        '/api/v1/references/guide/agency-identifier/')
    assert resp.status_code == 200
    assert resp.data['term'] == 'Agency Identifier'

    resp = client.get('/api/v1/references/guide/frumious-bandersnatch/')
    assert resp.status_code == 404

    resp = client.get(
        '/api/v1/references/guide/?data_act_term=Budget Authority Appropriated')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0
    for itm in resp.data['results']:
        assert itm['data_act_term'] == 'Budget Authority Appropriated'

    resp = client.get('/api/v1/references/guide/?plain__contains=Congress')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0
    for itm in resp.data['results']:
        assert 'congress' in itm['plain'].lower()
