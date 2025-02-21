import json
import pytest

from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def award_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        is_fpds=True,
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        type="A",
        recipient_location_city_name="BURBANK",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
        recipient_location_county_code="000",
        piid="piiiiid",
        pop_city_name="AUSTIN",
        pop_state_code="TX",
        pop_country_code="USA",
    )
    baker.make("search.AwardSearch", award_id=1, is_fpds=True, latest_transaction_id=1, piid="piiiiid", type="A")
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award_id=1,
        subaward_amount=123.45,
        action_date="2020-01-01",
        sub_place_of_perform_country_co="USA",
        sub_place_of_perform_state_code="TX",
        sub_place_of_perform_city_name="HOUSTON",
        sub_legal_entity_country_code="USA",
        sub_legal_entity_state_code="CA",
    )
    baker.make("references.PopCounty", state_name="California", county_number="000", latest_population=2403)
    baker.make("recipient.StateData", id="06-2020", fips="06", code="CA", name="California")
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


def test_geocode_filter_by_city(
    client, monkeypatch, elasticsearch_subaward_index, elasticsearch_transaction_index, award_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

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
                "spending_level": "subawards",
                "filters": {"place_of_performance_locations": [{"country": "USA", "state": "TX", "city": "Houston"}]},
            }
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
