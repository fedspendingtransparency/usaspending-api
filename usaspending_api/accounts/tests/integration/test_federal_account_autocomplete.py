import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.tests.autocomplete import check_autocomplete


@pytest.fixture
def fed_account_data(db):
    baker.make(FederalAccount, account_title="zzz", main_account_code="abc")
    baker.make(FederalAccount, account_title="###", main_account_code="789")
    baker.make(FederalAccount, main_account_code="XYZ")
    baker.make(
        FederalAccount,
        agency_identifier="999",
        main_account_code="0000",
        account_title="aaa",
        federal_account_code="999-0000",
    )


@pytest.mark.parametrize(
    "fields,value,expected",
    [
        (["main_account_code", "account_title"], "z", {"main_account_code": ["XYZ"], "account_title": ["zzz"]}),
        (["main_account_code"], "ab", {"main_account_code": ["abc"]}),
        (["main_account_code"], "789", {"main_account_code": ["789"]}),
        (["account_title"], "###", {"account_title": "###"}),
        (["account_title"], "ghi", {"account_title": []}),
        (["federal_account_code"], "999-0000", {"federal_account_code": "999-0000"}),
    ],
)
@pytest.mark.skip(reason="Deprecated endpoints; to remove later")
@pytest.mark.django_db
def test_awards_autocomplete(client, fed_account_data, fields, value, expected):
    """test partial-text search on awards."""

    check_autocomplete("federal_accounts", client, fields, value, expected)


@pytest.mark.skip(reason="Deprecated endpoints; to remove later")
@pytest.mark.django_db
def test_bad_awards_autocomplete_request(client):
    """Verify error on bad autocomplete request for awards."""

    resp = client.post("/api/v1/federal_accounts/autocomplete/", content_type="application/json", data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
