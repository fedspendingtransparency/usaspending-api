from usaspending_api.search.v2.es_sanitization import es_sanitize


def test_sanitizer():
    test_string = '+&|()[]{}*?:"<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ""
