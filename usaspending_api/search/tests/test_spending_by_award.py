import json

import pytest
from rest_framework import status

from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_award_subaward_success(client):

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "subawards": True,
            "fields": ["Sub-Award ID"],
            "sort": "Sub-Award ID",
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_success(client):

    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "subawards": False,
            "fields": ["Award ID"],
            "sort": "Award ID",
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK
