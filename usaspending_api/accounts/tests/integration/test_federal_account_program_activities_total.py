import json
import pytest
from rest_framework import status

url = "/api/v2/federal_accounts/{federal_account_code}/program_activities/total/"


@pytest.mark.django_db
def test_success(client, federal_accounts_test_data):
    resp = client.post("/api/v2/federal_accounts/000-0001/program_activities/total", content_type="application/json")

    expected_results = {
        "results": [
            {"obligations": 44112.0, "code": "00000000003", "name": "PARK 3", "type": "PARK"},
            {"obligations": 6000.0, "code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"obligations": 130.0, "code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
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


def test_success_with_filters(client, federal_accounts_test_data):
    request = {
        "filters": {
            "time_period": [{"start_date": "2020-01-01", "end_date": "2022-01-01"}],
            "program_activity": ["0001"],
        }
    }
    resp = client.post(
        "/api/v2/federal_accounts/000-0001/program_activities/total",
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [{"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"}],
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


@pytest.mark.django_db
def test_object_class_filter(client, federal_accounts_test_data):
    request = {"filters": {"object_class": ["moc1", "oc3"]}}
    resp = client.post(
        "/api/v2/federal_accounts/000-0001/program_activities/total",
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [
            {"obligations": 6000.0, "code": "00000000001", "name": "PARK 1", "type": "PARK"},
            {"obligations": 130.0, "code": "00000000002", "name": "PARK 2", "type": "PARK"},
            {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
        ],
        "page_metadata": {
            "page": 1,
            "total": 3,
            "limit": 10,
            "next": None,
            "previous": None,
            "hasNext": False,
            "hasPrevious": False,
        },
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_results


@pytest.mark.django_db
def test_pagination(client, program_activities_total_test_data):
    request = {"limit": 3, "page": 2}

    resp = client.post(
        "/api/v2/federal_accounts/000-0001/program_activities/total",
        content_type="application/json",
        data=json.dumps(request),
    )

    expected_results = {
        "results": [
            {"obligations": 1.0, "code": "0001", "name": "PAC/PAN 1", "type": "PAC/PAN"},
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
    assert resp.json() == expected_results
