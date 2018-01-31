import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_over_time_success(client):

    # test for needed filters
    resp = client.post(
        '/api/v2/search/spending_over_time',
        content_type='application/json',
        data=json.dumps({
            "group": "fiscal_year",
            "filters": {
                "keyword": "test"
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all filters
    resp = client.post(
        '/api/v2/search/spending_over_time',
        content_type='application/json',
        data=json.dumps({
            "group": "quarter",
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_over_time_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_over_time/',
        content_type='application/json',
        data=json.dumps({'group': 'fiscal_year'}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
