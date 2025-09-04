import json
import pytest
from rest_framework import status

url = "/api/v2/federal_accounts/{federal_account_code}/object_classes/total/"


@pytest.mark.django_db
def test_success(client, federal_accounts_test_data):
    resp = client.post(url.format(federal_account_code="000-0001"), content_type="application/json")

    expected_results = {
        "results": [
            {"code": "4", "name": "oc4", "obligations": 44112.0},
            {"code": "1", "name": "oc1", "obligations": 6000.0},
            {"code": "3", "name": "oc3", "obligations": 130.0},
            {"code": "2", "name": "oc2", "obligations": 1.0},
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
    assert resp.json() == expected_results


def test_success_with_multiple_time_periods(client, federal_accounts_test_data):
    request = {
        "filters": {
            "time_period": [
                {"start_date": "2020-01-01", "end_date": "2021-01-01"},
                {"start_date": "2023-01-01", "end_date": "2024-01-01"},
            ],
        }
    }

    resp = client.post(
        url.format(federal_account_code="000-0001"),
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [
            {"code": "1", "name": "oc1", "obligations": 6000.0},
            {"code": "4", "name": "oc4", "obligations": -1500.0},
        ],
        "page_metadata": {
            "page": 1,
            "total": 2,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


def test_multiple_filters(client, federal_accounts_test_data):
    request = {
        "filters": {
            "program_activity": ["0001", "00000000001"],
            "object_class": ["moc1", "moc2"],
            "time_period": [
                {"start_date": "2020-01-01", "end_date": "2021-01-01"},
                {"start_date": "2023-01-01", "end_date": "2024-01-01"},
            ],
        }
    }

    resp = client.post(
        url.format(federal_account_code="000-0001"),
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [
            {"code": "1", "name": "oc1", "obligations": 6000.0},
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
    assert resp.json() == expected_results
