import json
import pytest

from model_mommy import mommy


@pytest.fixture
def award_data_fixture(db):
    mommy.make("references.LegalEntity", legal_entity_id=1999999)
    mommy.make("awards.TransactionNormalized", id=1999999, award_id=1999999, action_date="2010-10-01", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1999999,
        legal_entity_zip5="abcde",
        legal_entity_city_name="ARLINGTON",
        legal_entity_state_code="VA",
        legal_entity_country_code="UNITED STATES",
        piid="IND12PB00323",
    )
    mommy.make(
        "awards.Award", id=1999999, latest_transaction_id=1999999, recipient_id=1999999, is_fpds=True, type="A", piid="IND12PB00323"
    )

    mommy.make("references.LegalEntity", legal_entity_id=2888888)
    mommy.make("awards.TransactionNormalized", id=2888888, award_id=2888888, action_date="2011-11-11", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2888888,
        legal_entity_zip5="abcde",
        legal_entity_city_name="BRISTOL",
        legal_entity_state_code=None,
        legal_entity_country_code="GBR",
        piid="0001",
    )
    mommy.make("awards.Award", id=2888888, latest_transaction_id=2888888, recipient_id=2888888, is_fpds=True, type="A", piid="0001")

    mommy.make("references.LegalEntity", legal_entity_id=3777777)
    mommy.make("awards.TransactionNormalized", id=3777777, award_id=3777777, action_date="2018-01-01", is_fpds=True, type="04")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3777777,
        legal_entity_zip5="abcde",
        legal_entity_city_name="PHILLIPSBURG",
        legal_entity_state_code="PA",
        piid="0002",
    )
    mommy.make("awards.Award", id=3777777, latest_transaction_id=3777777, recipient_id=3777777, is_fpds=True, type="04", piid="0002")
    mommy.make("references.LegalEntity", legal_entity_id=4666666)
    mommy.make("awards.TransactionNormalized", id=4666666, award_id=4666666, action_date="2011-11-11", is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4666666,
        legal_entity_zip5="abcde",
        legal_entity_city_name="BRISTOL",
        legal_entity_state_code="IL",
        legal_entity_country_code="USA",
        piid="0003",
    )
    mommy.make("awards.Award", id=4666666, latest_transaction_id=4666666, recipient_id=4666666, is_fpds=True, type="A", piid="0003")


@pytest.mark.django_db
def test_city_search_matches_found(client, db, award_data_fixture, elasticsearch_transaction_index):

    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "USA", "scope": "recipient_location"}, "search_text": "arli", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("arl") > -1


@pytest.mark.django_db
def test_city_search_no_matches(client, db, award_data_fixture, elasticsearch_transaction_index):

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


@pytest.mark.django_db
def test_city_search_special_characters(client, db, award_data_fixture, elasticsearch_transaction_index):

    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {"country_code": "USA", "scope": "recipient_location"},
        "search_text": 'arli+-&|!()[]{}^~*?:"/<>\\',  # Once special characters are stripped, this should just be 'arl'
        "limit": 20,
    }
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("arl") > -1


@pytest.mark.django_db
def test_city_search_non_usa(client, db, award_data_fixture, elasticsearch_transaction_index):
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


@pytest.mark.django_db
def test_city_search_foreign(client, db, award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()
    body = {"filter": {"country_code": "FOREIGN", "scope": "recipient_location"}, "search_text": "bri", "limit": 20}
    response = client.post("/api/v2/autocomplete/city", content_type="application/json", data=json.dumps(body))
    assert response.data["count"] == 1
    for entry in response.data["results"]:
        assert entry["city_name"].lower().find("bri") > -1


@pytest.mark.django_db
def test_city_search_nulls_are_usa(client, db, award_data_fixture, elasticsearch_transaction_index):
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
