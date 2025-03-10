import json

import pytest
from django.conf import settings
from model_bakery import baker

from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.references.v2.views.recipients import RecipientAutocompleteViewSet
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def recipient_data_fixture(db):
    baker.make(
        "recipient.RecipientProfile",
        id="01",
        recipient_level="C",
        recipient_hash="521bb024-054c-4c81-8615-372f81629664",
        uei="UEI-01",
        recipient_name="Spiderman",
        recipient_unique_id="hero1",  # aka duns
    )

    baker.make(
        "recipient.RecipientProfile",
        id="02",
        recipient_level="P",
        recipient_hash="a70b86c3-5a12-4623-963b-9d96c4810163",
        uei="UEI-02",
        recipient_name="Batman",
        recipient_unique_id="hero2",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="03",
        recipient_level="C",
        recipient_hash="a70b86c3-5a12-4623-963b-9d96c4810345",
        uei="UEI-03",
        recipient_name="Batman",
        recipient_unique_id="hero3",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="04",
        recipient_level="R",
        recipient_hash="9159db20-d2f7-42d4-88e2-a69759987520",
        uei="UEI-04",
        recipient_name="Superman",
        recipient_unique_id="hero4",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="05",
        recipient_level="C",
        recipient_hash="9159db20-d2f7-42d4-88e2-a69759987908",
        uei="UEI-05",
        recipient_name="sdfsdg",
        recipient_unique_id="hero5",
    )


def test_prepare_search_terms():
    view_set_instance = RecipientAutocompleteViewSet()

    request_data_with_levels = {"search_text": "test", "recipient_levels": ["C", "P"]}
    expected_result_with_levels = ["TEST", ["C", "P"]]
    assert view_set_instance._prepare_search_terms(request_data_with_levels) == expected_result_with_levels

    request_data_without_levels = {"search_text": "test"}
    expected_result_without_levels = ["TEST", []]
    assert view_set_instance._prepare_search_terms(request_data_without_levels) == expected_result_without_levels


def test_create_es_search():
    view_set_instance = RecipientAutocompleteViewSet()

    search_text = "test"
    recipient_levels = ["C", "P"]
    limit = 20

    expected_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "should": [
                                            {"match": {"recipient_level": "C"}},
                                            {"match": {"recipient_level": "P"}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "should": [
                                            {"match_phrase_prefix": {"recipient_name": {"query": "test", "boost": 5}}},
                                            {
                                                "match_phrase_prefix": {
                                                    "recipient_name.contains": {"query": "test", "boost": 3}
                                                }
                                            },
                                            {
                                                "match": {
                                                    "recipient_name": {"query": "test", "operator": "and", "boost": 1}
                                                }
                                            },
                                            {"match_phrase_prefix": {"uei": {"query": "test", "boost": 5}}},
                                            {"match_phrase_prefix": {"uei.contains": {"query": "test", "boost": 3}}},
                                            {"match": {"uei": {"query": "test", "operator": "and", "boost": 1}}},
                                            {"match_phrase_prefix": {"duns": {"query": "test", "boost": 5}}},
                                            {"match_phrase_prefix": {"duns.contains": {"query": "test", "boost": 3}}},
                                            {"match": {"duns": {"query": "test", "operator": "and", "boost": 1}}},
                                        ],
                                        "minimum_should_match": 1,
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        },
        "from": 0,
        "size": 20,
    }
    assert view_set_instance._create_es_search(search_text, recipient_levels, limit).to_dict() == expected_query

    search_text = "test"
    recipient_levels = []
    limit = 20
    expected_query = {
        "query": {
            "bool": {
                "should": [
                    {"match_phrase_prefix": {"recipient_name": {"query": "test", "boost": 5}}},
                    {"match_phrase_prefix": {"recipient_name.contains": {"query": "test", "boost": 3}}},
                    {"match": {"recipient_name": {"query": "test", "operator": "and", "boost": 1}}},
                    {"match_phrase_prefix": {"uei": {"query": "test", "boost": 5}}},
                    {"match_phrase_prefix": {"uei.contains": {"query": "test", "boost": 3}}},
                    {"match": {"uei": {"query": "test", "operator": "and", "boost": 1}}},
                    {"match_phrase_prefix": {"duns": {"query": "test", "boost": 5}}},
                    {"match_phrase_prefix": {"duns.contains": {"query": "test", "boost": 3}}},
                    {"match": {"duns": {"query": "test", "operator": "and", "boost": 1}}},
                ],
                "minimum_should_match": 1,
            }
        },
        "from": 0,
        "size": 20,
    }
    assert view_set_instance._create_es_search(search_text, recipient_levels, limit).to_dict() == expected_query


def test_query_elasticsearch(recipient_data_fixture, elasticsearch_recipient_index, monkeypatch):
    client = elasticsearch_recipient_index.client
    original_db_recipients_count = RecipientProfile.objects.count()
    assert original_db_recipients_count == 5

    setup_elasticsearch_test(monkeypatch, elasticsearch_recipient_index)
    assert client.indices.exists(elasticsearch_recipient_index.index_name)

    view_set_instance = RecipientAutocompleteViewSet()

    search_text = "sdfsdg"
    recipient_levels = ["C"]
    limit = 1

    elasticsearch_query = view_set_instance._create_es_search(search_text, recipient_levels, limit)
    expected_result = view_set_instance._query_elasticsearch(elasticsearch_query)

    response_actual = client.search(index=elasticsearch_recipient_index.index_name, body=elasticsearch_query.to_dict())
    format_response_actual = view_set_instance._parse_elasticsearch_response(response_actual)

    assert format_response_actual == expected_result


def test_parse_elasticsearch_response():
    view_set_instance = RecipientAutocompleteViewSet()

    hits_with_data = {
        "hits": {"hits": [{"_source": {"recipient_name": "Test", "uei": "UEI-01", "recipient_level": "C"}}]}
    }
    expected_results = [{"recipient_name": "Test", "uei": "UEI-01", "recipient_level": "C", "duns": None}]
    assert view_set_instance._parse_elasticsearch_response(hits_with_data) == expected_results

    hits_without_data = {"hits": {"hits": []}}
    expected_results = []
    assert view_set_instance._parse_elasticsearch_response(hits_without_data) == expected_results


def test_recipient_search_matches_found(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {"search_text": "superman", "recipient_levels": ["R"], "limit": 20}
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("superman") > -1


def test_recipient_partial_search_matches_found(
    client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index
):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {"search_text": "super", "recipient_levels": ["R"], "limit": 20}
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("superman") > -1


def test_recipient_search_multiple_recipient_levels(
    client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index
):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {"search_text": "batman", "recipient_levels": ["C", "P"], "limit": 20}
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 2
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("batman") > -1


def test_recipient_search_no_matches(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {"search_text": "nonexistent", "recipient_levels": ["R", "C", "D"], "limit": 20}
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached


def test_recipient_search_special_characters(
    client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index
):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {"search_text": "batman+()[]{}?<>\\", "recipient_levels": ["C", "P", "R"]}
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 2
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("batman") > -1
