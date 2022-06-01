import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import Cfda


@pytest.fixture
def cfda_data(db):
    baker.make(
        Cfda,
        program_number="10.117",
        popular_name="Biofuel Infrastructure Partnership (BIP)",
        program_title="Biofuel Infrastructure Partnership",
    )
    baker.make(
        Cfda,
        program_number="93.794",
        popular_name="",
        program_title="Reimbursement of State Costs for Provision of Part D Drugs",
    )
    baker.make(
        Cfda,
        program_number="10.577",
        popular_name="National Accuracy Clearinghouse (NAC) Pilot",
        program_title="SNAP Partnership Grant",
    )


@pytest.mark.django_db
def test_cfda_autocomplete_success(client, cfda_data):

    # test for program number
    resp = client.post(
        "/api/v2/autocomplete/cfda/", content_type="application/json", data=json.dumps({"search_text": "10.117"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["program_title"] == "Biofuel Infrastructure Partnership"

    # test for program title
    resp = client.post(
        "/api/v2/autocomplete/cfda/", content_type="application/json", data=json.dumps({"search_text": "Reimbursement"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["program_number"] == "93.794"

    # test for popular name
    resp = client.post(
        "/api/v2/autocomplete/cfda/", content_type="application/json", data=json.dumps({"search_text": "BIP"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["program_number"] == "10.117"


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Empty search string test"""
    resp = client.post(
        "/api/v2/autocomplete/psc/", content_type="application/json", data=json.dumps({"search_text": ""})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
