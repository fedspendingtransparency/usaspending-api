import json
import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import RefProgramActivity


@pytest.fixture
def program_activity_data(db):
    baker.make(RefProgramActivity, program_activity_code="0001", program_activity_name="ELECTRONICS")
    baker.make(RefProgramActivity, program_activity_code="0003", program_activity_name="MEAT")
    baker.make(RefProgramActivity, program_activity_code="0007", program_activity_name="BEANS")


@pytest.mark.django_db
def test_program_activity_autocomplete_success(client, program_activity_data):
    # test for program activity by code
    resp = client.post(
        "/api/v2/autocomplete/program_activity/",
        content_type="application/json",
        data=json.dumps({"search_text": "0003"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["program_activity_name"] == "MEAT"

    # test similar matches
    resp = client.post(
        "/api/v2/autocomplete/program_activity/",
        content_type="application/json",
        data=json.dumps({"search_text": "EA"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2
