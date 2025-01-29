import json

import pytest
from django.conf import settings
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def location_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        transaction_id=500,
        is_fpds=False,
        transaction_unique_id="TRANSACTION500",
        pop_country_name="UNITED STATES",
        pop_state_name="CALIFORNIA",
        pop_city_name="LOS ANGELES",
        pop_county_name="LOS ANGELES",
        pop_zip5=90001,
        pop_congressional_code_current="34",
        pop_congressional_code="34",
        pop_state_fips="11",
        pop_county_code="111",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="COLORADO",
        recipient_location_city_name="DENVER",
        recipient_location_county_name="DENVER",
        recipient_location_zip5=80012,
        recipient_location_congressional_code_current="01",
        recipient_location_congressional_code="01",
        recipient_location_state_fips="22",
        recipient_location_county_code="222",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=501,
        is_fpds=False,
        transaction_unique_id="TRANSACTION501",
        pop_country_name="DENMARK",
        pop_state_name=None,
        pop_city_name=None,
        pop_county_name=None,
        pop_zip5=None,
        pop_congressional_code_current=None,
        pop_congressional_code=None,
        pop_state_fips="33",
        pop_county_code="3333",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="GEORGIA",
        recipient_location_city_name="KINGSLAND",
        recipient_location_county_name="CAMDEN",
        recipient_location_zip5=31548,
        recipient_location_congressional_code_current="01",
        recipient_location_congressional_code="01",
        recipient_location_state_fips="44",
        recipient_location_county_code="444",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=502,
        is_fpds=False,
        transaction_unique_id="TRANSACTION502",
        pop_country_name="DENMARK",
        pop_state_name=None,
        pop_city_name=None,
        pop_county_name=None,
        pop_zip5=None,
        pop_congressional_code_current=None,
        pop_congressional_code=None,
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="FAKE STATE",
        recipient_location_city_name="FAKE CITY",
        recipient_location_county_name="FAKE COUNTY",
        recipient_location_zip5=75001,
        recipient_location_congressional_code_current="30",
        recipient_location_congressional_code="30",
    )
    baker.make("recipient.StateData", id=10, fips="06", code="CA", name="California", type="state", year=2024)
    baker.make("recipient.StateData", id=20, fips="08", code="CO", name="Colorado", type="state", year=2024)
    baker.make("recipient.StateData", id=30, fips="13", code="GA", name="Georgia", type="state", year=2024)


def test_exact_match(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    """Test searching ES and finding an exact match"""

    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location", content_type="application/json", data=json.dumps({"search_text": "denmark"})
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 1
    assert response.data["messages"] == [""]
    assert response.data["results"] == {"countries": ["DENMARK"]}


def test_multiple_types_of_matches(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    """Test query with multiple types of matches"""

    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location", content_type="application/json", data=json.dumps({"search_text": "den"})
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 4
    assert response.data["messages"] == [""]
    assert response.data["results"] == {
        "countries": ["DENMARK"],
        "cities": ["DENVER, COLORADO, UNITED STATES"],
        "counties": [
            {"county_fips": "22222", "county_name": "DENVER COUNTY, COLORADO, UNITED STATES"},
            {"county_fips": "44444", "county_name": "CAMDEN COUNTY, GEORGIA, UNITED STATES"},
        ],
    }


def test_congressional_district_results(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location",
        content_type="application/json",
        data=json.dumps({"search_text": "CA-34"}),
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 2
    assert response.data["messages"] == [""]
    assert response.data["results"] == {
        "districts_current": ["CA-34, CALIFORNIA, UNITED STATES"],
        "districts_original": ["CA-34, CALIFORNIA, UNITED STATES"],
    }


def test_no_results(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location",
        content_type="application/json",
        data=json.dumps({"search_text": "xyz"}),
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 0
    assert response.data["messages"] == [""]
    assert response.data["results"] == {}
