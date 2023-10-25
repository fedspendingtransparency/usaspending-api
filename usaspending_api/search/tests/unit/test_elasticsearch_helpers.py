from usaspending_api.search.v2.elasticsearch_helper import (
    es_minimal_sanitize,
    swap_keys,
)
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


def test_swap_keys():
    test = {
        "Recipient Name": "recipient_name",
        "Action Date": "action_date",
        "Transaction Amount": "federal_action_obligation",
    }

    results = swap_keys(test)

    assert results == {
        "recipient_name": "recipient_name",
        "action_date": "action_date",
        "federal_action_obligation": "federal_action_obligation",
    }
