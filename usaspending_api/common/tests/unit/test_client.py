from usaspending_api.common.elasticsearch.client import mock_es_client, _es_search
from usaspending_api.common.elasticsearch.mock_elasticsearch import basic_response

def test_basic_response():
    mock_es_client()
    value = _es_search("index","{isabody:true}",1)
    assert value == basic_response()