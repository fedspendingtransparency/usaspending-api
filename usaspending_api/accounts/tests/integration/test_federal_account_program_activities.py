import pytest
from rest_framework import status

url = "/api/v2/federal_accounts/{federal_account_code}/program_activities/{query_params}"


@pytest.mark.django_db
def test_success(client, federal_accounts_test_data):
    resp = client.get(url.format(federal_account_code="000-0001", query_params=""))
    expected_result = {
        "results": [
            {"code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
            {"code": "00000000003", "name": "PARK 3", "type": "PARK"},
            {"code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"code": "00000000001", "name": "PARK 1", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 4,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    resp = client.get(url.format(federal_account_code="000-0002", query_params=""))
    expected_result = {
        "results": [
            {"code": "00000000004", "name": "PARK 4", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 1,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_pagination(client, federal_accounts_test_data):
    resp = client.get(url.format(federal_account_code="000-0001", query_params="?limit=3&sort=name&order=asc"))
    expected_result = {
        "results": [
            {"code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
            {"code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"code": "00000000002", "name": "PARK 2", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 4,
            "limit": 3,
            "next": 2,
            "previous": None,
            "hasNext": True,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    resp = client.get(url.format(federal_account_code="000-0001", query_params="?limit=3&sort=name&order=asc&page=2"))
    expected_result = {
        "results": [
            {"code": "00000000003", "name": "PARK 3", "type": "PARK"},
        ],
        "page_metadata": {
            "page": 2,
            "total": 4,
            "limit": 3,
            "next": None,
            "previous": 1,
            "hasNext": False,
            "hasPrevious": True,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
