import json
import pytest

from rest_framework import status
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_geography_state_success(client, refresh_matviews):
    # test for required filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "place_of_performance",
            "geo_layer": "state",
            "filters": {
                'recipient_locations': [{'country': 'ABC'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "county",
            "geo_layer_filters": ["WA"],
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_county_success(client, refresh_matviews):
    # test for required filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "place_of_performance",
            "geo_layer": "county",
            "filters": {
                'recipient_locations': [{'country': 'ABC'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "county",
            "geo_layer_filters": ["01"],
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_congressional_success(client, refresh_matviews):
    # test for required filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "place_of_performance",
            "geo_layer": "district",
            "filters": {
                'recipient_locations': [{'country': 'ABC'}]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "district",
            "geo_layer_filters": ["01"],
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_failure(client, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({'scope': 'test', 'filters': {}}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_spending_by_geography_subawards_success(client, refresh_matviews):

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "county",
            "geo_layer_filters": ["01"],
            "filters": all_filters(),
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_geography_subawards_failure(client, refresh_matviews):

    resp = client.post(
        '/api/v2/search/spending_by_geography',
        content_type='application/json',
        data=json.dumps({
            "scope": "recipient_location",
            "geo_layer": "county",
            "geo_layer_filters": ["01"],
            "filters": all_filters(),
            "subawards": "string"
        }))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_incorrect_state(client, refresh_matviews):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'state',
            "geo_layer_filters": ["01"],
            'filters': all_filters()
        })
    )

    assert resp.data['results'][0]['display_name'] in ['Alabama', 'None']


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_incorrect_county(client, refresh_matviews):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'county',
            "geo_layer_filters": ["01"],
            'filters': all_filters()
        })
    )
    # raise Exception(resp.content)
    assert resp.data['results'][0]['display_name'] == 'County'


@pytest.mark.django_db
def test_spending_by_geography_incorrect_district(client, refresh_matviews):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'district',
            "geo_layer_filters": ["01"],
            'filters': all_filters()
        })
    )

    assert len(resp.data['results']) == 0
