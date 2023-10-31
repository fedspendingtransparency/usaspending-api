import json
import pytest

from rest_framework import status

from usaspending_api.awards.tests.integration.test_subawards import (
    create_subaward_test_data,
    subaward_1,
    subaward_2,
    subaward_3,
    subaward_10,
    subaward_11,
    subaward_12,
)


@pytest.mark.django_db
def test_subaward_no_params(client):
    create_subaward_test_data(subaward_1, subaward_2, subaward_3)
    resp = client.post("/api/v2/subawards/", content_type="application/json")
    assert resp.status_code == status.HTTP_200_OK
    assert len(json.loads(resp.content.decode("utf-8"))["results"]) == 3


@pytest.mark.django_db
def test_subaward_failure(client):
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": {"not": "a string or integer"}}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_subaward_limit(client):
    create_subaward_test_data(subaward_1, subaward_2, subaward_3)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"limit": 2}),
    )
    assert len(json.loads(resp.content.decode("utf-8"))["results"]) == 2


@pytest.mark.django_db
def test_subaward_filters(client):
    create_subaward_test_data(subaward_1, subaward_2, subaward_3, subaward_12, subaward_11)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"award_id": 99}),
    )
    assert len(json.loads(resp.content.decode("utf-8"))["results"]) == 4

    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"award_id": 88}),
    )
    results = json.loads(resp.content.decode("utf-8"))["results"]
    assert len(results) == 1
    assert results[0]["id"] == 12

    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"award_id": "generated_unique_award_id_for_88"}),
    )
    results = json.loads(resp.content.decode("utf-8"))["results"]
    assert len(results) == 1
    assert results[0]["id"] == 12


@pytest.mark.django_db
def test_subaward_sorting(client):
    create_subaward_test_data(subaward_1, subaward_12, subaward_10, subaward_2, subaward_3)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"sort": "description", "order": "asc"}),
    )
    results = json.loads(resp.content.decode("utf-8"))["results"]
    assert len(results) == 5
    assert results[0]["id"] == 1
    assert results[4]["id"] == 12
