import pytest
from rest_framework import status

url = "/api/v2/disaster/federal_account/spending/"


@pytest.mark.django_db
def test_federal_account_award_success(client, covid_faba_spending_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 1,
            "description": "gifts",
            "id": 21,
            "obligation": 100.0,
            "outlay": 111.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L", "N", "O"], spending_type="award")
    expected_results = [
        {
            "id": 21,
            "code": "000-0000",
            "description": "gifts",
            "award_count": 4,
            "obligation": 304.0,
            "outlay": 667.0,
            "total_budgetary_resources": None,
            "children": [
                {
                    "code": "2020/52",
                    "award_count": 1,
                    "description": "ferns",
                    "id": 24,
                    "obligation": 3.0,
                    "outlay": 333.0,
                    "total_budgetary_resources": None,
                },
                {
                    "code": "2020/98",
                    "award_count": 2,
                    "description": "evergreens",
                    "id": 23,
                    "obligation": 201.0,
                    "outlay": 223.0,
                    "total_budgetary_resources": None,
                },
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                },
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 4, "obligation": 304.0, "outlay": 667.0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_federal_account_award_empty(client, monkeypatch, covid_faba_spending_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_award_query(client, covid_faba_spending_data, helpers):

    resp = helpers.post_for_spending_endpoint(
        client, url, query="flowers", def_codes=["M", "L", "N", "O"], spending_type="award"
    )
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 1,
            "description": "gifts",
            "id": 21,
            "obligation": 100.0,
            "outlay": 111.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_outlay_calculations(client, covid_faba_spending_data, helpers):
    resp = helpers.post_for_spending_endpoint(
        client, url, query="evergreen", def_codes=["L"], spending_type="award", sort="outlay"
    )
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/98",
                    "award_count": 2,
                    "description": "evergreens",
                    "id": 23,
                    "obligation": 201.0,
                    "outlay": 223.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 2,
            "description": "gifts",
            "id": 21,
            "obligation": 201.0,
            "outlay": 223.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results
