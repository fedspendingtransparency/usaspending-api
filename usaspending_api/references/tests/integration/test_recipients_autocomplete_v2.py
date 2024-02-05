import json

import pytest
from django.conf import settings
from model_bakery import baker


@pytest.fixture
def recipient_data_fixture(db):
    baker.make(
        "recipient.RecipientProfile",
        id="01",
        recipient_level="C",
        recipient_hash="521bb024-054c-4c81-8615-372f81629664",
        uei="UEI-01",
        recipient_name="Spiderman",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="02",
        recipient_level="P",
        recipient_hash="a70b86c3-5a12-4623-963b-9d96c4810163",
        uei="UEI-02",
        recipient_name="Batman",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="03",
        recipient_level="C",
        recipient_hash="a70b86c3-5a12-4623-963b-9d96c4810345",
        uei="UEI-03",
        recipient_name="Batman",
    )

    baker.make(
        "recipient.RecipientProfile",
        id="04",
        recipient_level="R",
        recipient_hash="9159db20-d2f7-42d4-88e2-a69759987520",
        uei="UEI-04",
        recipient_name="Superman",
    )

def test_recipient_search_matches_found(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX ,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "search_text": "superman",
        "recipient_levels": ["R"],
        "limit": 20
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("superman") > -1

def test_recipient_search_multiple_recipient_levels(client, monkeypatch, recipient_data_fixture, elasticsearch_recipient_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX ,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "search_text": "batman",
        "recipient_levels": ["C", "P"],
        "limit": 20
    }
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
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX ,
    )
    elasticsearch_recipient_index.update_index()
    body = {
        "search_text": "batman+()[]{}?<>\\",
        "recipient_levels": ["C", "P", "R"]
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 2
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("batman") > -1