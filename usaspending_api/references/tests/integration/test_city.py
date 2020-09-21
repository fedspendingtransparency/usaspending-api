import json
import pytest

from django.conf import settings
from model_mommy import mommy


@pytest.fixture
def award_data_fixture(db):
    mommy.make("awards.TransactionNormalized", id=1, award_id=1, action_date="2010-10-01", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_zip5="abcde",
        legal_entity_city_name="ARLINGTON",
        legal_entity_state_code="VA",
        legal_entity_country_code="UNITED STATES",
        piid="IND12PB00323",
    )
    mommy.make("awards.Award", id=1, latest_transaction_id=1, is_fpds=True, type="A", piid="IND12PB00323")

    mommy.make("awards.TransactionNormalized", id=2, award_id=2, action_date="2011-11-11", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        legal_entity_zip5="abcde",
        legal_entity_city_name="BRISTOL",
        legal_entity_state_code=None,
        legal_entity_country_code="GBR",
        piid="0001",
    )
    mommy.make("awards.Award", id=2, latest_transaction_id=2, is_fpds=True, type="A", piid="0001")

    mommy.make("awards.TransactionNormalized", id=3, award_id=3, action_date="2018-01-01", is_fpds=True, type="04")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        legal_entity_zip5="abcde",
        legal_entity_city_name="PHILLIPSBURG",
        legal_entity_state_code="PA",
        piid="0002",
    )
    mommy.make("awards.Award", id=3, latest_transaction_id=3, is_fpds=True, type="04", piid="0002")
    mommy.make("awards.TransactionNormalized", id=4, award_id=4, action_date="2011-11-11", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        legal_entity_zip5="abcde",
        legal_entity_city_name="BRISTOL",
        legal_entity_state_code="IL",
        legal_entity_country_code="USA",
        piid="0003",
    )
    mommy.make("awards.Award", id=4, latest_transaction_id=4, is_fpds=True, type="A", piid="0003")

    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    mommy.make("references.RefCountryCode", country_code="GBR", country_name="UNITED KINGDOM")


def test_city_search_matches_found(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "USA", "scope": "recipient_location"}, "search_text": "arli", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("arl") > -1


def test_city_search_no_matches(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "USA", "scope": "recipient_location"}, "search_text": "bhqlg", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached

    body = {
        "filter": {"country_code": "USA", "scope": "recipient_location"},
        "search_text": "arlingtontownsburgplaceville",
        "limit": 20,
    }
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached


def test_city_search_special_characters(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {"country_code": "USA", "scope": "recipient_location"},
        "search_text": 'arli+|()[]{}?"<>\\',  # Once special characters are stripped, this should just be 'arl'
        "limit": 20,
    }
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("arl") > -1


def test_city_search_non_usa(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "GBR", "scope": "recipient_location"}, "search_text": "bri", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("bri") > -1

    body = {"filter": {"country_code": "USA", "scope": "recipient_location"}, "search_text": "bri", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("bri") > -1


def test_city_search_foreign(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "FOREIGN", "scope": "recipient_location"}, "search_text": "bri", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("bri") > -1


def test_city_search_nulls_are_usa(client, monkeypatch, award_data_fixture, elasticsearch_transaction_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "USA", "scope": "recipient_location"}, "search_text": "phil", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("phil") > -1

    body = {"filter": {"country_code": "FOREIGN", "scope": "recipient_location"}, "search_text": "phil", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 0
    for entry in response.data["results"]:
        assert False  # this should never be reached
