import json

import pytest
from elasticsearch_dsl import Q as ES_Q
from rest_framework import status

from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch


@pytest.mark.django_db(transaction=True)
def test_exact_match(client, elasticsearch_location_index):
    """Test searching ES and finding an exact match"""

    response = client.post(
        "/api/v2/autocomplete/location", content_type="application/json", data=json.dumps({"search_text": "denmark"})
    )

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 3
    assert response.data["count"] == 1
    assert response.data["messages"] == [""]
    assert response.data["results"] == {"countries": [{"country_name": "DENMARK"}]}


@pytest.mark.django_db(transaction=True)
def test_multiple_types_of_matches(client, elasticsearch_location_index):
    """Test query with multiple types of matches"""

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


@pytest.mark.django_db(transaction=True)
def test_congressional_district_results(client, elasticsearch_location_index):

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


@pytest.mark.django_db(transaction=True)
def test_zipcode_results(client, elasticsearch_location_index):

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


@pytest.mark.django_db(transaction=True)
def test_county_results(client, elasticsearch_location_index):

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
            {
                "county_name": "LOS ANGELES",
                "county_fips": "06107",
                "state_name": "CALIFORNIA",
                "country_name": "UNITED STATES",
            },
        ],
    }


@pytest.mark.django_db(transaction=True)
def test_no_results(client, elasticsearch_location_index):

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


@pytest.mark.django_db(transaction=True)
def test_verify_no_missing_fields(elasticsearch_location_index):
    """Verify that every document has all of the appropriate fields:
    - location
    - location_json
    - location_type
    """

    location_index_fields = ["location", "location_json", "location_type"]

    must_not_queries = [ES_Q("exists", field=field) for field in location_index_fields]
    must_not_exist_query = ES_Q("bool", must_not=must_not_queries, minimum_should_match=1)
    search = LocationSearch().query(must_not_exist_query)
    results = search.execute()

    assert len(results.hits) == 0


@pytest.mark.django_db(transaction=True)
def test_limits_by_location_type(client, elasticsearch_location_index):
    """Test that the endpoint returns (at most) 5 results of each `location_type` by default"""

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
