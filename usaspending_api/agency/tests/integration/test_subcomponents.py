import pytest

from model_mommy import mommy

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
def test_exclusion_bureau_codes(client, bureau_data):
    # Setup all Data (no bureau)
    mommy.make(
        "references.BureauTitleLookup",
        federal_account_code="",
        bureau_title="",
        bureau_slug="",
    )

    resp = client.get(url.format(toptier_code="004", filter="?fiscal_year=2021"))
    assert len(resp.json()) == 0

    # Setup bureau data
    expected_results = [
        {
            "name": "",
            "id": "",
            "total_obligations":    1,
            "total_outlays": 1,
            "total_budgetary_resources": 1,
        }
    ]
    resp = client.get(url.format(toptier_code="004", filter="?fiscal_year=2021"))
    assert resp.json()["results"] == expected_results
