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
    # assert len(resp.data["results"]) == 2    

    pytest.set_trace()    
    resp = client.get('/api/v1/references/guide/cooperative+agreement/')
    assert resp.status_code == 200
    # assert len(resp.data["results"]) == 2    
    
    resp = client.get('/api/v1/references/guide/frumious+bandersnatch')
    assert resp.status_code == 404
  
    
    resp = client.get('/api/v1/references/guide/autocomplete/?term=coo')
    assert resp.status_code == 200
    # assert len(resp.data["results"]) == 2    
   
    resp = client.get('/api/v1/guide/references/autocomplete/?plain=grant')
    assert resp.status_code == 200
    # assert len(resp.data["results"]) == 2    
