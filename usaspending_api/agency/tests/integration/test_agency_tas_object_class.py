import pytest


from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/treasury_account/{tas}/object_class/{query_params}"


@pytest.mark.django_db
def test_tas_object_class_success(client, monkeypatch, agency_account_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
        },
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "supplies",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "hvac",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 10000000.0,
                "name": "equipment",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1", "obligated_amount": 1.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # Tests non-existent tas
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-999"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }


@pytest.mark.django_db
def test_tas_object_class_multiple_pa_per_oc(client, monkeypatch, tas_mulitple_pas_per_oc, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 11000000.0,
                "name": "equipment",
                "obligated_amount": 11.0,
                "children": [
                    {"gross_outlay_amount": 1000000.0, "name": "NAME 2", "obligated_amount": 10.0},
                    {"gross_outlay_amount": 10000000.0, "name": "NAME 1", "obligated_amount": 1.0},
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_object_class_multiple_submission_years(client, agency_account_data):
    tas = "002-X-0000-000"
    submission_year = 2017
    query_params = f"?fiscal_year={submission_year}"
    resp = client.get(url.format(tas=tas, query_params=query_params))
    expected_result = {
        "fiscal_year": submission_year,
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 10000.0,
                "name": "interest",
                "obligated_amount": 1000.0,
                "children": [{"gross_outlay_amount": 10000.0, "name": "NAME 4", "obligated_amount": 1000.0}],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_with_no_object_class(client, monkeypatch, tas_with_no_object_class, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
