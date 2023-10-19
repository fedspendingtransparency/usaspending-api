import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import NAICS


@pytest.fixture
def naics_data(db):
    baker.make(NAICS, code="212113", description="Anthracite Mining")
    baker.make(NAICS, code="212112", description="Bituminous Coal Underground Mining")
    baker.make(NAICS, code="213111", description="Drilling Oil and Gas Wells")
    baker.make(NAICS, code="111331", description="Apple Orchards")


@pytest.mark.django_db
def test_naics_autocomplete_success(client, naics_data):

    # test for naics code
    resp = client.post(
        "/api/v2/autocomplete/naics/", content_type="application/json", data=json.dumps({"search_text": "212112"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["naics_description"] == "Bituminous Coal Underground Mining"

    # test for similarity
    resp = client.post(
        "/api/v2/autocomplete/naics/", content_type="application/json", data=json.dumps({"search_text": "Mining"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Empty search string"""
    resp = client.post(
        "/api/v2/autocomplete/naics/", content_type="application/json", data=json.dumps({"search_text": ""})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
