import json
import pytest
from rest_framework import status
from usaspending_api.accounts.tests.data.federal_account_total_data import federal_accounts_test_data

url = "/api/v2/federal_accounts/{federal_account_code}/object_classes/total/"

@pytest.mark.django_db
def test_success(client, federal_accounts_test_data):
    resp = client.get(url.format(federal_account_code="000-0001", content_type="application/json"))

    expected_results = {
        "results": [
            {"obligation": 1, "code": "001", "name": "name 1"}
        ],
        "page_metadata": {
            "page": 1,
            "total": 4,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        }
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results