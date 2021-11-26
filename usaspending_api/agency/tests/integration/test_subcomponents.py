import pytest

from rest_framework import status


url = "/api/v2/agency/{toptier_code}/subcomponents/{filter}"


@pytest.mark.django_db
def test_all_categories(client):
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2021"))

    expected_results = [
        {
            "name": "Test Bureau 1",
            "id": "test-bureau-1",
            "total_obligations": 10.0,
            "total_outlays": 100.0,
            "total_budgetary_resources": 1000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_year(client):
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 1",
            "abbreviation": "test-bureau-1",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_agency(client):
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 2",
            "abbreviation": "test-bureau-2",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }

    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_agency(client):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
