import pytest
from rest_framework import status

url = "/api/v2/disaster/federal_account/spending/"


@pytest.mark.django_db
def test_federal_account_success(client, generic_account_data, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="total")
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": None,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 511.0,
                    "total_budgetary_resources": 42580.0,
                }
            ],
            "code": "000-0000",
            "award_count": None,
            "description": "gifts",
            "id": 21,
            "obligation": 100.0,
            "outlay": 511.0,
            "total_budgetary_resources": 42580.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L", "N", "O"], spending_type="total")
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/52",
                    "award_count": None,
                    "description": "ferns",
                    "id": 24,
                    "obligation": 249.0,
                    "outlay": 0.0,
                    "total_budgetary_resources": 389480.0,
                },
                {
                    "code": "2020/98",
                    "award_count": None,
                    "description": "evergreens",
                    "id": 23,
                    "obligation": 761.0,
                    "outlay": 0.0,
                    "total_budgetary_resources": 1090370.0,
                },
                {
                    "code": "2020/99",
                    "award_count": None,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 511.0,
                    "total_budgetary_resources": 42580.0,
                },
            ],
            "code": "000-0000",
            "award_count": None,
            "description": "gifts",
            "id": 21,
            "obligation": 1110.0,
            "outlay": 511.0,
            "total_budgetary_resources": 1522430.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"obligation": 1110.0, "outlay": 511.0, "total_budgetary_resources": 1522430.0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_federal_account_empty(client, monkeypatch, helpers, generic_account_data):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="total")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_invalid_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"], spending_type="total")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"


@pytest.mark.django_db
def test_federal_account_invalid_defc_type(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100", spending_type="total")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_federal_account_missing_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, spending_type="total")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_federal_account_invalid_spending_type(client, monkeypatch, generic_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="total")
    assert resp.status_code == status.HTTP_200_OK

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="gibberish")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'spending_type' is outside valid values ['total', 'award']"


@pytest.mark.django_db
def test_federal_account_missing_spending_type(client, monkeypatch, generic_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'spending_type' is a required field"
