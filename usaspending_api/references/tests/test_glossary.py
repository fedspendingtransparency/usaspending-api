import pytest

from django.core.management import call_command


@pytest.fixture()
def glossary_data(db):
    call_command('load_glossary')


def test_glossary_endpoint(client, glossary_data):

    # Sorry to squash these together, but I don't want to load the guide data
    # multiple times

    resp = client.get('/api/v1/references/glossary/')
    assert resp.status_code == 200
    assert len(resp.data["results"]) > 70

    resp = client.get(
        '/api/v1/references/glossary/agency-identifier/')
    assert resp.status_code == 200
    assert resp.data['term'] == 'Agency Identifier'

    resp = client.get('/api/v1/references/glossary/frumious-bandersnatch/')
    assert resp.status_code == 404

    resp = client.get(
        '/api/v1/references/glossary/?data_act_term=Budget Authority Appropriated')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0
    for itm in resp.data['results']:
        assert itm['data_act_term'] == 'Budget Authority Appropriated'

    resp = client.get('/api/v1/references/glossary/?plain__contains=Congress')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0
    for itm in resp.data['results']:
        assert 'congress' in itm['plain'].lower()
