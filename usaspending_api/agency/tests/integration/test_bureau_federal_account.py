import pytest

from rest_framework import status

url = "/api/v2/agency/{toptier_code}/sub_components/{bureau_slug}/{query_params}"


@pytest.mark.django_db
def test_federal_account_list_success(client, monkeypatch, bureau_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    resp = client.get(
        url.format(
            toptier_code="001",
            bureau_slug="test-bureau-1",
            query_params=f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}",
        )
    )
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "001",
        "bureau_slug": "test-bureau-1",
        "totals": {
            "total_obligations": 1.0,
            "total_outlays": 10.0,
            "total_budgetary_resources": 100.0,
        },
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
            "limit": 10,
        },
        "results": [
            {
                "name": "FA 1",
                "id": "001-0000",
                "total_obligations": 1.0,
                "total_outlays": 10.0,
                "total_budgetary_resources": 100.0,
            }
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_alternate_year(client, bureau_data):
    resp = client.get(url.format(toptier_code="001", bureau_slug="test-bureau-1", query_params="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "FA 1",
            "id": "001-0000",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="002", bureau_slug="test-bureau-2", query_params="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "FA 2",
            "id": "002-0000",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="XXX", bureau_slug="test-bureau", query_params="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", bureau_slug="1234@#$@#", query_params="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
