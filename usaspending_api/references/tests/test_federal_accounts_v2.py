import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def fixture_data(db):
    mommy.make(
        FederalAccount,
    )


@pytest.mark.django_db
def test_federal_accounts_endpoint_exists(client, fixture_data):

    # test for exact match
    resp = client.post(
        '/api/v2/references/federal_accounts/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_200_OK
