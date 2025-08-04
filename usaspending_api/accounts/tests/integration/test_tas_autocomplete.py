import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.tests.autocomplete import check_autocomplete


@pytest.fixture
def tas_data(db):
    baker.make(TreasuryAppropriationAccount, tas_rendering_label="zzz", main_account_code="abc")
    baker.make(TreasuryAppropriationAccount, tas_rendering_label="###", main_account_code="789")
    baker.make(TreasuryAppropriationAccount, main_account_code="XYZ")


@pytest.mark.parametrize(
    "fields,value,expected",
    [
        (
            ["main_account_code", "tas_rendering_label"],
            "z",
            {"main_account_code": ["XYZ"], "tas_rendering_label": ["zzz"]},
        ),
        (["main_account_code"], "ab", {"main_account_code": ["abc"]}),
        (["main_account_code"], "12", {"main_account_code": ["123"]}),
        (["main_account_code"], "789", {"main_account_code": ["789"]}),
        (["tas_rendering_label"], "###", {"tas_rendering_label": "###"}),
        (["tas_rendering_label"], "ghi", {"tas_rendering_label": []}),
    ],
)
@pytest.mark.skip(reason="Deprecated endpoints; to remove later")
@pytest.mark.django_db
def test_awards_autocomplete(client, tas_data, fields, value, expected):
    """test partial-text search on awards."""

    check_autocomplete("tas", client, fields, value, expected)


@pytest.mark.skip(reason="Deprecated endpoints; to remove later")
@pytest.mark.django_db
def test_bad_awards_autocomplete_request(client):
    """Verify error on bad autocomplete request for awards."""

    resp = client.post("/api/v1/tas/autocomplete/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
