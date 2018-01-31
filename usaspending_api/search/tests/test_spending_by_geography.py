import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_success(client):

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
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({'scope': 'test', 'filter': {}}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_incorrect_state(client):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'state',
            'filter': {}
        })
    )

    assert resp.data['results'][0]['display_name'] in ['Alabama', 'None']


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_incorrect_county(client):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'county',
            'filter': {}
        })
    )

    assert resp.data['results'][0]['display_name'] == 'County'


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_geography_incorrect_district(client):
    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({
            'scope': 'place_of_performance',
            'geo_layer': 'district',
            'filter': {}
        })
    )

    assert len(resp.data['results']) == 0
