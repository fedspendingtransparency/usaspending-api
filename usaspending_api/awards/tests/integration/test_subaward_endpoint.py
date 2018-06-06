import json

import pytest
from rest_framework import status


@pytest.mark.django_db
def test_subaward_success(client, refresh_matviews):

    resp = client.post(
        '/api/v2/subawards/',
        content_type='application/json',
        data=json.dumps(
            {
            "order": "desc", 
            "limit": 100 
            })
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_subaward_failure(client, refresh_matviews):

    resp = client.post(
        '/api/v2/subawards/',
        content_type='application/json',
        data=json.dumps(
            {
            "order": "desc", 
            "limit": 100,
            "award_id":"not an integer", 
            })
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

