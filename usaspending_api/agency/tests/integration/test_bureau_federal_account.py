import pytest

from rest_framework import status

url = "/api/v2/agency/{toptier_code}/subcomponents/{bureau_slug}/{query_params}"


@pytest.mark.django_db
def test_federal_account_list_success(client, monkeypatch, bureau_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    resp = client.get(url.format(toptier_code="001", bureau_slug="test-bureau", query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "001",
        "bureau_slug": "test-bureau",
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
    print(resp.json())
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
