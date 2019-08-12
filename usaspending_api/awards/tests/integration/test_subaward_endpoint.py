import json
import pytest
from rest_framework import status

# Core Django imports
from django_mock_queries.query import MockModel


# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.awards.tests.unit.test_subawards import subaward_1, subaward_2, subaward_3, subaward_12


@pytest.mark.django_db
def test_subaward_success(client, refresh_matviews):

    resp = client.post(
        "/api/v2/subawards/", content_type="application/json", data=json.dumps({"order": "desc", "limit": 100})
    )
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_subaward_failure(client, refresh_matviews):

    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": "not an integer"}),
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_subaward_query_1(client, refresh_matviews, mock_matviews_qs):
    mock_model_1 = MockModel(**subaward_1)
    mock_model_2 = MockModel(**subaward_2)
    mock_model_3 = MockModel(**subaward_3)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3])
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": 99}),
    )
    assert len(json.loads(resp.content.decode("utf-8"))["results"]) == 3


@pytest.mark.django_db
def test_subaward_query_2(client, refresh_matviews, mock_matviews_qs):
    mock_model_4 = MockModel(**subaward_12)

    add_to_mock_objects(mock_matviews_qs, [mock_model_4])
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": 88}),
    )
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["id"] == 12
