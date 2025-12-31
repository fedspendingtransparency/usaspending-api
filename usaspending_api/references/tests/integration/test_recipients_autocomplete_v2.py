import json

import pytest
from django.conf import settings
from elasticsearch_dsl import AttrDict
from model_bakery import baker

from usaspending_api.references.v2.views.recipients import RecipientAutocompleteViewSet


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
    view_set_instance.search_text = "test"
    view_set_instance.recipient_levels = ["C", "P"]
    view_set_instance.limit = 20

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
        "aggs": {
            "filter_recipient_name": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"recipient_name": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"recipient_name.contains": {"query": "test", "boost": 3}}},
                            {"match": {"recipient_name": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {"unique_recipient_name": {"terms": {"field": "recipient_name.keyword", "size": 20}}},
            },
            "filter_uei": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"uei": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"uei.contains": {"query": "test", "boost": 3}}},
                            {"match": {"uei": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {
                    "unique_uei": {
                        "terms": {"field": "uei.keyword", "size": 20},
                        "aggs": {
                            "recipient_details": {"top_hits": {"size": 1, "_source": {"includes": "recipient_name"}}}
                        },
                    }
                },
            },
            "filter_duns": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"duns": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"duns.contains": {"query": "test", "boost": 3}}},
                            {"match": {"duns": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {
                    "unique_duns": {
                        "terms": {"field": "duns.keyword", "size": 20},
                        "aggs": {
                            "recipient_details": {"top_hits": {"size": 1, "_source": {"includes": "recipient_name"}}}
                        },
                    }
                },
            },
        },
        "size": 1,
    }
    assert view_set_instance._create_es_search().to_dict() == expected_query

    view_set_instance.search_text = "test"
    view_set_instance.recipient_levels = []
    view_set_instance.limit = 20
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
        "aggs": {
            "filter_recipient_name": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"recipient_name": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"recipient_name.contains": {"query": "test", "boost": 3}}},
                            {"match": {"recipient_name": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {"unique_recipient_name": {"terms": {"field": "recipient_name.keyword", "size": 20}}},
            },
            "filter_uei": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"uei": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"uei.contains": {"query": "test", "boost": 3}}},
                            {"match": {"uei": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {
                    "unique_uei": {
                        "terms": {"field": "uei.keyword", "size": 20},
                        "aggs": {
                            "recipient_details": {"top_hits": {"size": 1, "_source": {"includes": "recipient_name"}}}
                        },
                    }
                },
            },
            "filter_duns": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"duns": {"query": "test", "boost": 5}}},
                            {"match_phrase_prefix": {"duns.contains": {"query": "test", "boost": 3}}},
                            {"match": {"duns": {"query": "test", "operator": "and", "boost": 1}}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "aggs": {
                    "unique_duns": {
                        "terms": {"field": "duns.keyword", "size": 20},
                        "aggs": {
                            "recipient_details": {"top_hits": {"size": 1, "_source": {"includes": "recipient_name"}}}
                        },
                    }
                },
            },
        },
        "size": 1,
    }
    assert view_set_instance._create_es_search().to_dict() == expected_query


def test_parse_elasticsearch_response():
    view_set_instance = RecipientAutocompleteViewSet()
    sample_response = AttrDict(
        {
            "filter_recipient_name": {"unique_recipient_name": [{"key": "SAMPLE RECIPIENT"}]},
            "filter_uei": {
                "unique_uei": [
                    {"key": "SAMPLE UEI", "recipient_details": [{"recipient_name": "SAMPLE RECIPIENT WITH UEI"}]}
                ]
            },
            "filter_duns": {
                "unique_duns": [
                    {"key": "SAMPLE DUNS", "recipient_details": [{"recipient_name": "SAMPLE RECIPIENT WITH DUNS"}]}
                ]
            },
        }
    )
    expected_results = [
        {"recipient_name": "SAMPLE RECIPIENT", "uei": None, "recipient_level": None, "duns": None},
        {"recipient_name": "SAMPLE RECIPIENT WITH UEI", "uei": "SAMPLE UEI", "recipient_level": None, "duns": None},
        {"recipient_name": "SAMPLE RECIPIENT WITH DUNS", "uei": None, "recipient_level": None, "duns": "SAMPLE DUNS"},
    ]
    assert view_set_instance._parse_elasticsearch_response(sample_response) == expected_results

    hits_without_data = AttrDict(
        {
            "filter_recipient_name": {"unique_recipient_name": []},
            "filter_uei": {"unique_uei": []},
            "filter_duns": {"unique_duns": []},
        }
    )
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
    assert response.data["count"] == 1
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
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("batman") > -1
