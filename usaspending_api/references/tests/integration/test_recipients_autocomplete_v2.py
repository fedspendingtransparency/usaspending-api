import json
import pytest

from django.conf import settings
from model_bakery import baker

@pytest.fixture
def transaction_data_fixture(db):
    baker.make(
        "search.RecipientSearch", 
        id=1,
        recipient_hash="sample_hash_1",
        recipient_level="R",
        recipient_name="Sample Recipient 1",
        uei="UEI1"
    )

    baker.make(
        "search.RecipientSearch",  
        id=2,
        recipient_hash="sample_hash_2",
        recipient_level="C",
        recipient_name="Sample Recipient 2",
        uei="UEI2"
    )

def test_recipient_search_matches_found(client, monkeypatch, transaction_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {
        "search_text": "kimberly",
        "recipient_levels": ["R", "C", "D"],
        "limit": 20
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("kimberly") > -1

def test_recipient_search_no_matches(client, monkeypatch, transaction_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {
        "search_text": "nonexistent",
        "recipient_levels": ["R", "C", "D"],
        "limit": 20
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached

def test_recipient_search_special_characters(client, monkeypatch, transaction_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.RecipientSearch._index_name",
        settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {
        "search_text": 'spec|()[]{}?"<>\\',
        "recipient_levels": ["R", "C", "D"],
        "limit": 20,
    }
    response = client.post("/api/v2/autocomplete/recipient", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["recipient_name"].lower().find("spec") > -1


