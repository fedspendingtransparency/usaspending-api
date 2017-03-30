import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.tests.autocomplete import check_autocomplete


@pytest.fixture
def fed_account_data(db):
    mommy.make(
        FederalAccount,
        account_title='zzz',
        main_account_code='abc')
    mommy.make(
        FederalAccount,
        account_title='###',
        main_account_code='789')
    mommy.make(FederalAccount, main_account_code='XYZ')


@pytest.mark.parametrize("fields,value,expected", [
    (['main_account_code', 'account_title'], 'z', {
        'main_account_code': ['XYZ'],
        'account_title': ['zzz']
    }),
    (['main_account_code'], 'ab', {
        'main_account_code': ['abc']
    }),
    (['main_account_code'], '789', {
        'main_account_code': ['789']
    }),
    (['account_title'], '###', {
        'account_title': '###'
    }),
    (['account_title'], 'ghi', {
        'account_title': []
    }),
])
@pytest.mark.django_db
def test_awards_autocomplete(client, fed_account_data, fields, value, expected):
    """test partial-text search on awards."""

    check_autocomplete('federal_accounts', client, fields, value, expected)


def test_bad_awards_autocomplete_request(client):
    """Verify error on bad autocomplete request for awards."""

    resp = client.post(
        '/api/v1/federal_accounts/autocomplete/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
