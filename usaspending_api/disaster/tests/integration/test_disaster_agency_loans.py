import pytest
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

url = "/api/v2/disaster/agency/loans/"


@pytest.mark.django_db
def test_basic_success(client, disaster_account_data, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M", "N", "O", "P"])
    expected_results = [
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "children": [],
            "award_count": 1,
            "obligation": 2000.0,
            "outlay": 20000.0,
            "face_value_of_loan": 333.0,
        },
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "face_value_of_loan": 333.0, "obligation": 2000.0, "outlay": 20000.0}
    assert resp.json()["totals"] == expected_totals

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_award_type_codes(client, disaster_account_data, elasticsearch_award_index, monkeypatch, helpers):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["07", "08"], def_codes=["L", "M", "N", "O", "P"], spending_type="award"
    )
    expected_results = [
        {
            "id": 1,
            "code": "007",
            "description": "Agency 007",
            "award_count": 1,
            "obligation": 2000.0,
            "outlay": 20000.0,
            "face_value_of_loan": 333.0,
            "children": [
                {
                    "id": 1,
                    "code": "1007",
                    "description": "Subtier 1007",
                    "award_count": 1,
                    "obligation": 2000.0,
                    "outlay": 20000.0,
                    "face_value_of_loan": 333.0,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["08"], def_codes=["L", "M", "N", "O", "P"], spending_type="award"
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_empty(client, monkeypatch, helpers, elasticsearch_award_index, generic_account_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_invalid_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"


@pytest.mark.django_db
def test_invalid_defc_type(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_missing_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_invalid_award_type_codes(client, monkeypatch, helpers, elasticsearch_award_index, disaster_account_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = helpers.post_for_spending_endpoint(client, url, award_type_codes=["ZZ", "08"], def_codes=["L", "M"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|award_type_codes' is outside valid values ['07', '08']"
