import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search \
    import budget_function_data, all_filters


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_award_type_success(client, budget_function_data):

    # test for filters
    resp = client.post(
        '/api/v2/search/spending_by_award_count/',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "award_type_codes": ["A", "B", "C"]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    resp = client.post(
        '/api/v2/search/spending_by_award_count',
        content_type='application/json',
        data=json.dumps({
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.skip
@pytest.mark.django_db
def test_spending_by_award_type_failure(client):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_award_count/',
        content_type='application/json',
        data=json.dumps({'test': {}, 'filter': {}}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
