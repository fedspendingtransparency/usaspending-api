import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.references.models import PSC


@pytest.fixture
def psc_data(db):
    baker.make(PSC, code="3605", description="FOOD PRODUCTS MACHINE & EQ")
    baker.make(PSC, code="8435", description="FOOTWEAR, WOMEN'S")
    baker.make(PSC, code="6250", description="BALLASTS, LAMPHOLDERS, AND STARTERS")


@pytest.mark.django_db
def test_psc_autocomplete_success(client, psc_data):

    # test for psc by code
    resp = client.post(
        "/api/v2/autocomplete/psc/", content_type="application/json", data=json.dumps({"search_text": "8435"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    assert resp.data["results"][0]["psc_description"] == "FOOTWEAR, WOMEN'S"

    # test for similar matches
    resp = client.post(
        "/api/v2/autocomplete/psc/", content_type="application/json", data=json.dumps({"search_text": "FOO"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2


@pytest.mark.django_db
def test_naics_autocomplete_failure(client):
    """Test empty search string."""
    resp = client.post(
        "/api/v2/autocomplete/psc/", content_type="application/json", data=json.dumps({"search_text": ""})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
