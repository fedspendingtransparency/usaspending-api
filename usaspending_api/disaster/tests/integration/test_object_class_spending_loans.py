import pytest
from rest_framework import status

url = "/api/v2/disaster/object_class/loans/"


@pytest.mark.django_db
def test_basic_object_class_award_success(client, basic_object_class_faba_with_loan_value, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"])
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": 1,
            "obligation": 1.0,
            "outlay": 0.0,
            "children": [
                {
                    "id": 1,
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": 1,
                    "obligation": 1.0,
                    "outlay": 0.0,
                    "face_value_of_loan": 5.0,
                }
            ],
            "face_value_of_loan": 5.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "face_value_of_loan": 5.0, "obligation": 1.0, "outlay": 0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_object_class_spending_filters_on_defc(client, basic_object_class_faba_with_loan_value, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert len(resp.json()["results"]) == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"])
    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_object_class_spending_query_filter(client, basic_object_class_faba_with_loan_value, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M", "N"], query="structures")
    expected_results = [
        {
            "id": "003",
            "code": "003",
            "description": "Acquisition of assets",
            "award_count": 5,
            "obligation": 111.0,
            "outlay": 111.0,
            "face_value_of_loan": 555.0,
            "children": [
                {
                    "id": 3,
                    "code": "0003",
                    "description": "Land and structures",
                    "award_count": 5,
                    "obligation": 111.0,
                    "outlay": 111.0,
                    "face_value_of_loan": 555.0,
                }
            ],
        }
    ]
    assert resp.status_code == 200
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M", "N"], query="supplies")
    expected_results = [
        {
            "id": "002",
            "code": "002",
            "description": "Contractual services and supplies",
            "award_count": 5,
            "obligation": 111.0,
            "outlay": 111.0,
            "face_value_of_loan": 555.0,
            "children": [
                {
                    "id": 2,
                    "code": "0002",
                    "description": "Research and development contracts",
                    "award_count": 5,
                    "obligation": 111.0,
                    "outlay": 111.0,
                    "face_value_of_loan": 555.0,
                }
            ],
        }
    ]
    assert resp.status_code == 200
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_bad_defc(client, basic_object_class_faba_with_loan_value, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["BAD"])

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Field 'filter|def_codes' is outside valid values ['A', 'L', 'M', 'N']"
