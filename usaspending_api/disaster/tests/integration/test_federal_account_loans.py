import pytest
from rest_framework import status

url = "/api/v2/disaster/federal_account/loans/"


@pytest.mark.django_db
def test_federal_account_loans_success(client, generic_account_data, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L", "N", "O"])
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/52",
                    "award_count": 1,
                    "description": "ferns",
                    "face_value_of_loan": 4444.0,
                    "id": 24,
                    "outlay": 333.0,
                    "obligation": 3.0,
                },
                {
                    "code": "2020/98",
                    "award_count": 2,
                    "description": "evergreens",
                    "face_value_of_loan": 4444.0,
                    "id": 23,
                    "outlay": 223.0,
                    "obligation": 201.0,
                },
            ],
            "code": "000-0000",
            "award_count": 3,
            "description": "gifts",
            "face_value_of_loan": 8888.0,
            "outlay": 556.0,
            "obligation": 204.0,
            "id": 21,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_federal_account_loans_empty(client, monkeypatch, helpers, generic_account_data):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc_type(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_federal_account_loans_missing_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
