import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/recipient/loans/"


@pytest.mark.django_db
def test_recipient_loans_success(client, monkeypatch, helpers, elasticsearch_award_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["N"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L"])
    expected_results = [
        {
            "children": [
                {"code": "2020/52", "count": 1, "description": "ferns", "face_value_of_loan": 4444.0, "id": 24},
                {"code": "2020/98", "count": 2, "description": "evergreens", "face_value_of_loan": 4444.0, "id": 23},
            ],
            "code": "000-0000",
            "count": 3,
            "description": "gifts",
            "face_value_of_loan": 8888.0,
            "id": 21,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M"])
    expected_results = [
        {
            "children": [
                {"code": "2020/52", "count": 1, "description": "ferns", "face_value_of_loan": 4444.0, "id": 24},
                {"code": "2020/98", "count": 2, "description": "evergreens", "face_value_of_loan": 4444.0, "id": 23},
            ],
            "code": "000-0000",
            "count": 3,
            "description": "gifts",
            "face_value_of_loan": 8888.0,
            "id": 21,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_federal_account_loans_empty(client, monkeypatch, helpers, awards_and_transactions):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc(client, helpers, awards_and_transactions):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O']"


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc_type(client, helpers, awards_and_transactions):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_federal_account_loans_missing_defc(client, helpers, awards_and_transactions):
    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
