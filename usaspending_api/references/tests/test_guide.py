import pytest

from django.core.management import call_command

@pytest.fixture()
def guide_data(db):
    call_command('load_guide')
    
    
def test_guide_endpoint(client, guide_data):
    
    # Sorry to squash these together, but I don't want to load the guide data 
    # multiple times 
    
    resp = client.get('/api/v1/references/guide/')
    assert resp.status_code == 200
    assert len(resp.data["results"]) > 70

    resp = client.get('http://localhost:8000/api/v1/references/guide/agency-identifier/')
    assert resp.status_code == 200
    assert resp.data['term'] == 'Agency Identifier'
    
    resp = client.get('/api/v1/references/guide/frumious-bandersnatch/')
    assert resp.status_code == 404
  
    resp = client.get('/api/v1/references/guide/?data_act_term=Budget Authority Appropriated')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0 
    for itm in resp.data['results']:
        assert itm['data_act_term'] == 'Budget Authority Appropriated'
   
    resp = client.get('/api/v1/references/guide/?plain__contains=Congress')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0 
    for itm in resp.data['results']:
        assert 'congress' in itm['plain'].lower()

    # case-insensitivity fail?        
    """
    resp = client.get('/api/v1/references/guide/?plain__contains=congress')
    assert resp.status_code == 200
    assert len(resp.data['results']) > 0 
    for itm in resp.data['results']:
        assert 'congress' in itm['plain'].lower()
   
    """
