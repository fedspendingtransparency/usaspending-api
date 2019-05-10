from usaspending_api.common.elasticsearch.client import mock_es_client, _es_search
from usaspending_api.common.elasticsearch.mock_elasticsearch import stored_values
from elasticsearch import ConnectionError

def test_basic_response():
    mock_es_client("simple_search_by_city")
    value = _es_search("index","{isabody:true}",1)
    assert value == stored_values["simple_search_by_city"]()


def test_connection_error():
    mock_es_client("connection_error")
    assert  _es_search("index","{isabody:true}",9) == ConnectionError
