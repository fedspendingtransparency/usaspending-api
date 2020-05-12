import json
import pytest


from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/{code}/budget_function/"


@pytest.mark.django_db
def test_budget_function_list_success(client, agency_account_data):
    resp = client.post(url.format(code="007"), content_type="application/json")
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2017}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2017,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            }
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # this agency has a record but the amounts are both 0, so we expect this return no results
    body = {"fiscal_year": 2016}
    resp = client.post(url.format(code="010"), content_type="application/json", data=json.dumps(body))
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
def test_budget_function_list_too_early(client, agency_account_data):
    body = {"fiscal_year": 2007}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_budget_function_list_future(client, agency_account_data):
    body = {"fiscal_year": current_fiscal_year() + 1}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_budget_function_list_bad_sort(client, agency_account_data):
    body = {"sort": "not valid"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_budget_function_list_bad_order(client, agency_account_data):
    body = {"order": "not valid"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_budget_function_list_sort_by_name(client, agency_account_data):
    body = {"fiscal_year": 2020, "order": "asc", "sort": "name"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2020, "order": "desc", "sort": "name"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_budget_function_list_sort_by_obligated_amount(client, agency_account_data):
    body = {"fiscal_year": 2020, "order": "asc", "sort": "obligated_amount"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2020, "order": "desc", "sort": "obligated_amount"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_budget_function_list_sort_by_gross_outlay_amount(client, agency_account_data):
    body = {"fiscal_year": 2020, "order": "asc", "sort": "gross_outlay_amount"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2020, "order": "desc", "sort": "gross_outlay_amount"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_budget_function_list_search(client, agency_account_data):
    body = {"fiscal_year": 2020, "filter": "NAME 3"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2020, "filter": "AME 2"}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 10,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": False, "next": None, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_budget_function_list_pagination(client, agency_account_data):
    body = {"fiscal_year": 2020, "limit": 2, "page": 1}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 2,
        "messages": [],
        "page_metadata": {"hasNext": True, "hasPrevious": False, "next": 2, "page": 1, "previous": None},
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "NAME 3",
                "obligated_amount": 100.0,
                "children": [{"gross_outlay_amount": 100000.0, "name": "NAME 3A", "obligated_amount": 100.0}],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "NAME 2",
                "obligated_amount": 10.0,
                "children": [{"gross_outlay_amount": 1000000.0, "name": "NAME 2A", "obligated_amount": 10.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    body = {"fiscal_year": 2020, "limit": 2, "page": 2}
    resp = client.post(url.format(code="007"), content_type="application/json", data=json.dumps(body))
    expected_result = {
        "fiscal_year": 2020,
        "toptier_code": "007",
        "limit": 2,
        "messages": [],
        "page_metadata": {"hasNext": False, "hasPrevious": True, "next": None, "page": 2, "previous": 1},
        "results": [
            {
                "gross_outlay_amount": 10000000.0,
                "name": "NAME 1",
                "obligated_amount": 1.0,
                "children": [{"gross_outlay_amount": 10000000.0, "name": "NAME 1A", "obligated_amount": 1.0}],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
