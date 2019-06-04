from usaspending_api.search.v2.elasticsearch_helper import es_sanitize


def test_sanitizer():
    test_string = '+-&|!()[]{}^~*?:"/<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ''
