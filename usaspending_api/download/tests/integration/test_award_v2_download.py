import json
import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.download.lookups import JOB_STATUS


@pytest.fixture
def award_download_data(db):
    for js in JOB_STATUS:
        mommy.make("download.JobStatus", job_status_id=js.id, name=js.name, description=js.desc)

    mommy.make(
        "awards.Award",
        id=1,
        category="contracts",
        piid="piid1",
        fpds_agency_id=123,
        type="A",
        generated_unique_award_id="CONT_piid1_123",
    )

    mommy.make(
        "awards.Award",
        id=2,
        category="grants",
        fain="fain",
        type="04",
        generated_unique_award_id="ASST_NON_FAIN_12D2",
    )


@pytest.mark.django_db
def test_download_contract_endpoint(client, award_download_data):
    resp = client.post("/api/v2/download/contract", content_type="application/json", data=json.dumps({"award_id": 1}))

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]


@pytest.mark.django_db
def test_download_assistance_endpoint(client, award_download_data):
    resp = client.post("/api/v2/download/assistance", content_type="application/json", data=json.dumps({"award_id": 2}))

    assert resp.status_code == status.HTTP_200_OK
    assert ".zip" in resp.json()["url"]
