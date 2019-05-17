import json
import pytest
import logging #TODO: remove this

from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.v2.views.city import CityAutocompleteViewSet

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


def test_city_search_matches_found(client,db, award_data_fixture, elasticsearch_transaction_index):
    logger = logging.getLogger("console")
    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "primary_place_of_performance"
        },
        "search_text": "arl",
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 15
    for entry in response.data['results']:
        assert entry['city_name'].lower().find('arl') > -1


def test_city_search_no_matches(client,db, award_data_fixture, elasticsearch_transaction_index):
    logger = logging.getLogger("console")
    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "primary_place_of_performance"
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
            "scope": "primary_place_of_performance"
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

def test_city_search_special_characters(client,db, award_data_fixture, elasticsearch_transaction_index):
    logger = logging.getLogger("console")
    elasticsearch_transaction_index.update_index()
    body = {
        "filter": {
            "country_code": "USA",
            "scope": "primary_place_of_performance"
        },
        "search_text": "arl+-&|!()[]{}^~*?:\"/<>\\",  # Once special characters are stripped, this should just be 'arl'
        "limit": 20
    }
    response = client.post('/api/v2/autocomplete/city',
                           content_type='application/json',
                           data=json.dumps(body))
    assert response.data['count'] == 15
    for entry in response.data['results']:
        assert entry['city_name'].lower().find('arl') > -1