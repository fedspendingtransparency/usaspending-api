import json

import pytest
from django.conf import settings
from elasticsearch_dsl import Q as ES_Q
from model_bakery import baker
from rest_framework import status

from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch


@pytest.fixture
def location_data_fixture(db):
    denmark = baker.make("references.RefCountryCode", country_code="DNK", country_name="DENMARK")
    france = baker.make("references.RefCountryCode", country_code="FRA", country_name="FRANCE")
    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")

    baker.make("recipient.StateData", id="1", code="CO", name="Colorado")
    baker.make("recipient.StateData", id="2", code="CA", name="California")
    baker.make("recipient.StateData", id="3", code="TX", name="Texas")
    baker.make("recipient.StateData", id="4", code="IL", name="Illinois")
    baker.make("recipient.StateData", id="5", code="OK", name="Oklahoma")

    baker.make("references.CityCountyStateCode", id=1, feature_name="Denver", state_alpha="CO")
    baker.make("references.CityCountyStateCode", id=2, feature_name="Texas A City", state_alpha="TX")
    baker.make("references.CityCountyStateCode", id=3, feature_name="Texas B City", state_alpha="TX")
    baker.make("references.CityCountyStateCode", id=4, feature_name="Texas C City", state_alpha="IL")
    baker.make("references.CityCountyStateCode", id=5, feature_name="Texas D City", state_alpha="OK")
    baker.make("references.CityCountyStateCode", id=6, feature_name="Texas E City", state_alpha="TX")
    baker.make("references.CityCountyStateCode", id=7, feature_name="Texas F City", state_alpha="TX")
    baker.make("references.CityCountyStateCode", id=8, county_name="Los Angeles", state_alpha="CA")

    baker.make("references.ZipsGrouped", zips_grouped_id=1, zip5="90210", state_abbreviation="CA")
    baker.make("references.ZipsGrouped", zips_grouped_id=2, zip5="90211", state_abbreviation="CA")

    baker.make(
        "search.TransactionSearch",
        transaction_id=500,
        is_fpds=False,
        transaction_unique_id="TRANSACTION500",
        pop_country_name=denmark.country_name,
        pop_country_code=denmark.country_code,
        pop_city_name="COPENHAGEN",
        recipient_location_country_name=france.country_name,
        recipient_location_country_code=france.country_code,
        recipient_location_city_name="PARIS",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=501,
        is_fpds=False,
        transaction_unique_id="TRANSACTION501",
        pop_country_name="UNITED STATES",
        pop_country_code="USA",
        pop_state_code="CA",
        pop_congressional_code_current="34",
        pop_congressional_code="34",
        recipient_location_country_name="UNITED STATES",
        recipient_location_country_code="USA",
        recipient_location_state_code="CA",
        recipient_location_congressional_code_current="34",
        recipient_location_congressional_code="34",
    )


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
    assert response.data["results"] == {"countries": [{"country_name": "DENMARK"}]}


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
    assert response.data["count"] == 2
    assert response.data["messages"] == [""]
    assert response.data["results"] == {
        "countries": [{"country_name": "DENMARK"}],
        "cities": [{"city_name": "DENVER", "state_name": "COLORADO", "country_name": "UNITED STATES"}],
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
        "districts_current": [{"current_cd": "CA-34", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"}],
        "districts_original": [{"original_cd": "CA-34", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"}],
    }


def test_zipcode_results(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location",
        content_type="application/json",
        data=json.dumps({"search_text": "90210"}),
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 1
    assert response.data["messages"] == [""]
    assert response.data["results"] == {
        "zip_codes": [
            {"zip_code": "90210", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        ]
    }


def test_county_results(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location",
        content_type="application/json",
        data=json.dumps({"search_text": "los angeles"}),
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 1
    assert response.data["messages"] == [""]
    assert response.data["results"] == {
        "counties": [
            {"county_name": "LOS ANGELES", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        ],
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


def test_verify_no_missing_fields(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    """Verify that every document has all of the appropriate fields:
    - location
    - location_json
    - location_type
    """

    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    location_index_fields = ["location", "location_json", "location_type"]

    must_not_queries = [ES_Q("exists", field=field) for field in location_index_fields]
    must_not_exist_query = ES_Q("bool", must_not=must_not_queries, minimum_should_match=1)
    search = LocationSearch().query(must_not_exist_query)
    results = search.execute()

    assert len(results.hits) == 0


def test_limits_by_location_type(client, monkeypatch, location_data_fixture, elasticsearch_location_index):
    """Test that the endpoint returns (at most) 5 results of each `location_type` by default"""

    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.LocationSearch._index_name",
        settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_location_index.update_index()

    response = client.post(
        "/api/v2/autocomplete/location", content_type="application/json", data=json.dumps({"search_text": "texas"})
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 6
    assert response.data["messages"] == [""]

    assert 0 < len(response.data["results"]["cities"]) <= 5
    assert 0 < len(response.data["results"]["states"]) <= 5

    assert "cities" in response.data["results"].keys()
    assert "states" in response.data["results"].keys()
