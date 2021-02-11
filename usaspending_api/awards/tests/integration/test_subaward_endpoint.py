import json
import pytest

from rest_framework import status

from usaspending_api.awards.tests.unit.test_subawards import (
    create_subaward_test_data,
    subaward_1,
    subaward_2,
    subaward_3,
    subaward_12,
)


@pytest.mark.django_db
def test_subaward_success(client):

    resp = client.post("/api/v2/subawards/?order=desc&limit=2")
    assert resp.status_code == status.HTTP_200_OK
    print("hi")

    print(resp.content)


@pytest.mark.django_db
def test_subaward_failure(client):

    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "award_id": {"not": "a string or integer"}}),
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_subaward_query_1(client):
    create_subaward_test_data(subaward_1, subaward_2, subaward_3)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": 99}),
    )
    assert len(json.loads(resp.content.decode("utf-8"))["results"]) == 3


@pytest.mark.django_db
def test_subaward_query_2(client):
    create_subaward_test_data(subaward_12)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": 88}),
    )
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["id"] == 12


@pytest.mark.django_db
def test_subaward_query_3(client):
    create_subaward_test_data(subaward_12)
    resp = client.post(
        "/api/v2/subawards/",
        content_type="application/json",
        data=json.dumps({"order": "desc", "limit": 100, "award_id": "generated_unique_award_id_for_88"}),
    )
    assert json.loads(resp.content.decode("utf-8"))["results"][0]["id"] == 12
