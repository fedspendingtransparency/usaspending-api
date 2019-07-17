import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.tests.autocomplete import check_autocomplete
from usaspending_api.references.models import LegalEntity


@pytest.fixture
def recipients_data(db):
    mommy.make(LegalEntity, recipient_name="Lunar Colonization Society", recipient_unique_id="LCS123")
    mommy.make(LegalEntity, recipient_name="Cerean Mineral Extraction Corp.", recipient_unique_id="CMEC")
    mommy.make(LegalEntity, recipient_name="LOOK NO DUNS.", recipient_unique_id=None)


@pytest.mark.parametrize(
    "fields,value,expected",
    [
        (["recipient_name"], "ext", {"recipient_name": ["Cerean Mineral Extraction Corp."]}),
        (["recipient_name", "recipient_unique_id"], "123", {"recipient_name": [], "recipient_unique_id": ["LCS123"]}),
    ],
)
@pytest.mark.django_db
def test_recipients_autocomplete(client, recipients_data, fields, value, expected):
    """test partial-text search on recipients."""

    check_autocomplete("references/recipients", client, fields, value, expected)


@pytest.mark.django_db
def test_bad_recipients_autocomplete_request(client):
    """Verify error on bad autocomplete request for recipients."""

    resp = client.post(
        "/api/v1/references/recipients/autocomplete/", content_type="application/json", data=json.dumps({})
    )
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_recipient_autocomplete_null_duns_exclusion(client, recipients_data):
    resp = client.post(
        "/api/v1/references/recipients/autocomplete/",
        content_type="application/json",
        data=json.dumps({"fields": ["recipient_name"], "value": "LOOK NO DUNS"}),
    )
    assert len(resp.data["results"]["recipient_name"]) == 0
