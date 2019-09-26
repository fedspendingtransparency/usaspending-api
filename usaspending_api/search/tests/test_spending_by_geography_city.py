import json
import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def award_data_fixture(db):
    mommy.make("references.LegalEntity", legal_entity_id=1)
    mommy.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True, type="A")
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_city_name="BURBANK",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="AUSTIN",
        place_of_performance_state="TX",
        place_of_perform_country_c="USA",
    )
    mommy.make("awards.Award", id=1, is_fpds=True, latest_transaction_id=1, piid="piiiiid", recipient_id=1, type="A")
    mommy.make(
        "awards.Subaward",
        id=1,
        award_id=1,
        amount=123.45,
        pop_country_code="USA",
        pop_state_code="TX",
        pop_city_name="HOUSTON",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
    )


def test_geocode_filter_by_city(client, award_data_fixture, refresh_matviews):

    # Place of performance that does exist.
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "filters": {"recipient_locations": [{"country": "USA", "state": "CA", "city": "Burbank"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    # Place of performance that doesn't exist.
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "filters": {"recipient_locations": [{"country": "USA", "state": "TX", "city": "Houston"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0

    # Place of performance that does exist.
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "filters": {"place_of_performance_locations": [{"country": "USA", "state": "TX", "city": "Austin"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    # Recipient location that doesn't exist.
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "recipient_location",
                "geo_layer": "state",
                "filters": {"place_of_performance_locations": [{"country": "USA", "state": "CA", "city": "Burbank"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 0

    # Filter subawards.
    resp = client.post(
        "/api/v2/search/spending_by_geography",
        content_type="application/json",
        data=json.dumps(
            {
                "scope": "place_of_performance",
                "geo_layer": "state",
                "subawards": True,
                "filters": {"place_of_performance_locations": [{"country": "USA", "state": "TX", "city": "Houston"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
