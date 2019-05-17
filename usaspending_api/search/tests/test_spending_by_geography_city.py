import json
import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def award_data_fixture(db):
    mommy.make("references.Location", location_id=1, location_country_code="USA", state_code="TX")
    mommy.make("references.LegalEntity", legal_entity_id=1)
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        action_date="2010-10-01",
        award_id=1,
        is_fpds=True,
        place_of_performance_id=1,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_city_name="Burbank",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="Houston"
    )
    mommy.make(
        "awards.Award", id=1, is_fpds=True, latest_transaction_id=1, piid="piiiiid", recipient_id=1, type="A",
        place_of_performance_id=1
    )


def test_geocode_filter_locations(client, award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "state",
            "filters": {
                'recipient_locations': [{'country': 'USA', 'state': 'CA', 'city': 'Burbank'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "state",
            "filters": {
                'recipient_locations': [{'country': 'USA', 'state': 'TX', 'city': 'Houston'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "place_of_performance",
            "geo_layer": "state",
            "filters": {
                'place_of_performance_locations': [{'country': 'USA', 'state': 'TX', 'city': 'Houston'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "state",
            "filters": {
                'place_of_performance_locations': [{'country': 'USA', 'state': 'CA', 'city': 'Burbank'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0
