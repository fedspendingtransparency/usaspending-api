import pytest
from rest_framework import status

url = "/api/v2/disaster/object_class/spending/"


@pytest.mark.django_db
def test_basic_object_class_award_success(client, basic_faba_with_object_class, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
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
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "obligation": 1.0, "outlay": 0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_object_class_counts_awards(client, basic_faba_with_object_class, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["N"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0]["award_count"] == 2
    assert len(resp.json()["results"][0]["children"]) == 1


@pytest.mark.django_db
def test_object_class_spending_filters_on_defc(client, basic_faba_with_object_class, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert len(resp.json()["results"]) == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_object_class_query(client, basic_faba_with_object_class, helpers):
    resp = helpers.post_for_spending_endpoint(
        client, url, query="001 name", def_codes=["A", "M", "N"], spending_type="award"
    )
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
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "obligation": 1.0, "outlay": 0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_outlay_calculations(client, basic_faba_with_object_class, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, query="003 name", def_codes=["O"], spending_type="award")
    expected_results = [
        {
            "id": "003",
            "code": "003",
            "description": "003 name",
            "award_count": 7,
            "obligation": 3.0,
            "outlay": 60.0,
            "children": [
                {
                    "id": 5,
                    "code": "00033",
                    "description": "00033 name",
                    "award_count": 2,
                    "obligation": 1.0,
                    "outlay": 30.0,
                },
                {
                    "id": 4,
                    "code": "00032",
                    "description": "00032 name",
                    "award_count": 3,
                    "obligation": 1.0,
                    "outlay": 20.0,
                },
                {
                    "id": 3,
                    "code": "00031",
                    "description": "00031 name",
                    "award_count": 2,
                    "obligation": 1.0,
                    "outlay": 10.0,
                },
            ],
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 7, "obligation": 3.0, "outlay": 60.0}
    assert resp.json()["totals"] == expected_totals
