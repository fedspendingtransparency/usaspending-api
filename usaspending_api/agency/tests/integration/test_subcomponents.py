import pytest

from rest_framework import status

url = "/api/v2/agency/{toptier_code}/sub_components/{filter}"


@pytest.mark.django_db
def test_success(client, bureau_data, helpers):
    resp = client.get(url.format(toptier_code="001", filter=f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}"))

    expected_results = [
        {
            "name": "Test Bureau 1",
            "id": "test-bureau-1",
            "total_obligations": 1.0,
            "total_outlays": 10.0,
            "total_budgetary_resources": 100.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_year(client, bureau_data):
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 1",
            "id": "test-bureau-1",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 2",
            "id": "test-bureau-2",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_invalid_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(url.format(toptier_code="999", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND
    
@pytest.mark.django_db
def test_exclusion_bureau_codes(client, bureau_data):
    resp = client.get(url.format(toptier_code="047", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Supply and Technology Activities",
            "id": "supply-and-technology-activities",
            "total_obligations": 13637597132.27,
            "total_outlays": 11202524114.57,
            "total_budgetary_resources": 14256844957.76
        },
        {
            "name": "Real Property Activities",
            "id": "real-property-activities",
            "total_obligations": 11539267245.6,
            "total_outlays": 11231748700.58,
            "total_budgetary_resources": 16299870242.56
        },
        {
            "name": "General Activities",
            "id": "general-activities",
            "total_obligations": 994729034.63,
            "total_outlays": 1001502482.77,
            "total_budgetary_resources": 1195992440.05
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results
