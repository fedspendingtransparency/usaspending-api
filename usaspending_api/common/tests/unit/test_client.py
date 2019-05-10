from usaspending_api.common.elasticsearch.client import mock_es_client, _es_search

def test_stuff():
    mock_es_client()
    _es_search("index","{isabody:true}",1)