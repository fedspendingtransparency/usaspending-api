import pytest


from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/{code}/object_class/{query_params}"


@pytest.mark.django_db
def test_object_class_list_success(client, agency_account_data):
    resp = client.get(url.format(code="007", query_params=""))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2017"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2017,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2016"
    resp = client.get(url.format(code="010", query_params=query_params))
    expected_result = {
        "fiscal_year": 2016,
        "toptier_code": "010",
        "limit": 10,
        "messages": [
            "Account data powering this endpoint were first collected in "
            "FY2017 Q2 under the DATA Act; as such, there are no data "
            "available for prior fiscal years."
        ],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_too_early(client, agency_account_data):
    query_params = "?fiscal_year=2007"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_object_class_list_future(client, agency_account_data):
    query_params = f"?fiscal_year={current_fiscal_year() + 1}"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_object_class_list_bad_sort(client, agency_account_data):
    query_params = "?sort=not%20valid"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_object_class_list_bad_order(client, agency_account_data):
    query_params = "?order=not%20valid"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_object_class_list_specific(client, agency_account_data):
    query_params = "?fiscal_year=2017"
    resp = client.get(url.format(code="008", query_params=query_params))
    expected_result = {
        "fiscal_year": 2017,
        "toptier_code": "008",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [{"gross_outlay_amount": 10000.0, "name": "interest", "obligated_amount": 1000.0}],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2018"
    resp = client.get(url.format(code="008", query_params=query_params))
    expected_result = {
        "fiscal_year": 2018,
        "toptier_code": "008",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [{"gross_outlay_amount": 1000.0, "name": "supplies", "obligated_amount": 10000.0}],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_ignore_duplicates(client, agency_account_data):
    query_params = "?fiscal_year=2019"
    resp = client.get(url.format(code="009", query_params=query_params))
    expected_result = {
        "fiscal_year": 2019,
        "toptier_code": "009",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [{"gross_outlay_amount": 11.0, "name": "interest", "obligated_amount": 11000000.0}],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_sort_by_name(client, agency_account_data):
    query_params = "?fiscal_year=2020&order=asc&sort=name"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2020&order=desc&sort=name"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_sort_by_obligated_amount(client, agency_account_data):
    query_params = "?fiscal_year=2020&order=asc&sort=obligated_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2020&order=desc&sort=obligated_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_sort_by_gross_outlay_amount(client, agency_account_data):
    query_params = "?fiscal_year=2020&order=asc&sort=gross_outlay_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2020&order=desc&sort=gross_outlay_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_search(client, agency_account_data):
    query_params = "?fiscal_year=2020&filter=supplies"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [{"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0}],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2020&filter=uipm"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [{"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0}],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_object_class_list_pagination(client, agency_account_data):
    query_params = "?fiscal_year=2020&limit=2&page=1"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 2,
        "messages": [],
        "page_metadata": {"hasNext": True, "hasPrevious": False, "next": 2, "page": 1, "previous": None},
        "results": [
            {"gross_outlay_amount": 100000.0, "name": "supplies", "obligated_amount": 100.0},
            {"gross_outlay_amount": 1000000.0, "name": "hvac", "obligated_amount": 10.0},
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2020&limit=2&page=2"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 2,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": True, "next": None, "page": 2, "previous": 1},
        "results": [{"gross_outlay_amount": 10000000.0, "name": "equipment", "obligated_amount": 1.0}],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
