import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search \
    import budget_function_data, all_filters


@pytest.mark.django_db
def test_spending_by_geography_success(client, budget_function_data):

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


@pytest.mark.django_db
def test_spending_by_geography_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_geography/',
        content_type='application/json',
        data=json.dumps({'scope': 'test', 'filter': {}}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
