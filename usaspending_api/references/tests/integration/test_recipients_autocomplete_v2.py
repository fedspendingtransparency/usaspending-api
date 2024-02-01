import json

import pytest
from django.conf import settings
from model_bakery import baker


@pytest.fixture
def recipient_data_fixture(db):
    baker.make(
        "search.AwardSearch",
        id=17394129117,
        recipient_hash="6a9f72ac-1d21-a1c6-e21c-82cde9d0a68eS",
        recipient_level="C",
        recipient_name="EMERZIAN WOODWORKING INC",
        uei="C111JJBMS328"
    )

    baker.make(
        "search.AwardSearch",
        id=17378750196,
        recipient_hash="3e69fe7a-fd9f-be78-2f2d-fb3d7bf296d9",
        recipient_level="R",
        recipient_name="ADMINISTRATION LOUISIANA DIVISION OF",
        uei="M9TEQW6ANUJ4"
    )

def test_recipient_search_matches_found(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.S_RECIPIENTS_QUERY_ALIAS_PREFIX ,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "search_text": "emerzian",
        "recipient_levels": ["C"],
        "limit": 20
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("emerzian") > -1

def test_recipient_search_no_matches(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "search_text": "nonexistent",
        "recipient_levels": ["R", "C", "D"],
        "limit": 20
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached

def test_recipient_search_special_characters(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "filter": {"country_code": "USA", "scope": "recipient_location"},
        "search_text": 'emer+|()[]{}?"<>\\',  # Once special characters are stripped, this should just be 'arl'
        "limit": 20,
    }
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("emer") > -1
