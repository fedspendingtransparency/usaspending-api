import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data(db):
    mommy.make(
        LegalEntity,
        legal_entity_id="271286",
        recipient_unique_id="048122469",
        recipient_name="EVERGREEN TERRACE STARTUP, INC.",
    )
    mommy.make(
        LegalEntity,
        legal_entity_id="342445",
        recipient_unique_id="136725434",
        parent_recipient_unique_id="",
        recipient_name="The Human Partnership",
    )
    mommy.make(
        LegalEntity,
        legal_entity_id="342467",
        recipient_unique_id="298712981",
        parent_recipient_unique_id="136725434",
        recipient_name="The Human Childship",
    )
    mommy.make(
        LegalEntity,
        legal_entity_id="274178",
        recipient_unique_id="078757313",
        recipient_name="STARTUP JUNKIE CONSULTING, LLC",
    )


@pytest.mark.django_db
def test_recipient_autocomplete_success(client, recipients_data):

    # Test on search_text using string
    resp = client.post(
        "/api/v2/autocomplete/recipient/", content_type="application/json", data=json.dumps({"search_text": "Human"})
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["recipient_id_list"]) == 2
    # Legal Entity IDs for Human Childship and Human Partnership
    assert 342445 in resp.data["results"]["recipient_id_list"]
    assert 342467 in resp.data["results"]["recipient_id_list"]

    # Test on on search_text using DUNS (recipient_unique_id)
    resp = client.post(
        "/api/v2/autocomplete/recipient/",
        content_type="application/json",
        data=json.dumps({"search_text": "078757313"}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]["recipient_id_list"]) == 1
    assert 274178 in resp.data["results"]["recipient_id_list"]


@pytest.mark.django_db
def test_recipient_autocomplete_failure(client):
    """Empty string test"""
    resp = client.post(
        "/api/v2/autocomplete/recipient/", content_type="application/json", data=json.dumps({"search_text": ""})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
