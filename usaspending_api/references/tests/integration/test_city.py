import json
import pytest

from model_mommy import mommy


@pytest.fixture
def award_data_fixture(db):
    mommy.make(
        'references.LegalEntity',
        legal_entity_id=1
    )
    mommy.make(
        'awards.TransactionNormalized',
        id=1,
        award_id=1,
        action_date='2010-10-01',
        is_fpds=True,
        type='A'
    )
    mommy.make(
        'awards.TransactionFPDS',
        transaction_id=1,
        legal_entity_zip5='abcde',
        legal_entity_city_name="ARLINGTON",
        legal_entity_state_code="VA",
        legal_entity_country_code="UNITED STATES",
        piid='IND12PB00323'
    )
    mommy.make(
        'awards.Award',
        id=1,
        latest_transaction_id=1,
        recipient_id=1,
        is_fpds=True,
        type='A',
        piid='IND12PB00323'
    )


@pytest.mark.django_db
def test_city_search_matches_found(client, db, award_data_fixture, elasticsearch_transaction_index):

    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "recipient_location"
        },
        "search_text": "arli",
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 1
    for entry in response.data['results']:
        assert entry['city_name'].lower().find('arl') > -1


@pytest.mark.django_db
def test_city_search_no_matches(client, db, award_data_fixture, elasticsearch_transaction_index):

    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "recipient_location"
        },
        "search_text": "bhqlg",
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 0
    for entry in response.data['results']:
        assert False  # this should never be reached

    body = {
        "filter": {
            "country_code": "USA",
            "scope": "recipient_location"
        },
        "search_text": "arlingtontownsburgplaceville",
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 0
    for entry in response.data['results']:
        assert False  # this should never be reached


@pytest.mark.django_db
def test_city_search_special_characters(client, db, award_data_fixture, elasticsearch_transaction_index):

    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "recipient_location"
        },
        "search_text": "arli+-&|!()[]{}^~*?:\"/<>\\",  # Once special characters are stripped, this should just be 'arl'
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 1
    for entry in response.data['results']:
        assert entry['city_name'].lower().find('arl') > -1
