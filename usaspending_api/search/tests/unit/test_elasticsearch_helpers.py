from usaspending_api.search.v2.elasticsearch_helper import es_minimal_sanitize
from usaspending_api.search.v2.es_sanitization import es_sanitize


def test_es_sanitize():
    test_string = '+|()[]{}?"<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ""
    test_string = "!-^~/&:*"
    processed_string = es_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/\&\:\*"


def test_es_minimal_sanitize():
    test_string = "https://www.localhost:8000/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"https\:\/\/www.localhost\:8000\/"
    test_string = "!-^~/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/"
