from usaspending_api.common.elasticsearch.client import es_sanitize


def test_sanitizer():
    test_string = '+-&|!()[]{}^~*?:"/<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ''
