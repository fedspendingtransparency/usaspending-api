import pytest


from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/{code}/federal_account/{query_params}"


@pytest.mark.django_db
def test_federal_account_list_success(client, monkeypatch, agency_account_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    resp = client.get(url.format(code="007", query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = "?fiscal_year=2017"
    resp = client.get(url.format(code="008", query_params=query_params))
    expected_result = {
        "fiscal_year": 2017,
        "toptier_code": "008",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 10000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 1000.0,
                "children": [
                    {
                        "gross_outlay_amount": 10000.0,
                        "name": "TA 2",
                        "code": "002-X-0000-000",
                        "obligated_amount": 1000.0,
                    }
                ],
            }
        ],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # this agency has a record but the amounts are both 0, so we expect this return no results
    query_params = "?fiscal_year=2016"
    resp = client.get(url.format(code="010", query_params=query_params))
    expected_result = {
        "fiscal_year": 2016,
        "toptier_code": "010",
        "messages": [
            "Account data powering this endpoint were first collected in "
            "FY2017 Q2 under the DATA Act; as such, there are no data "
            "available for prior fiscal years."
        ],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
            "limit": 10,
        },
        "results": [],
    }
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_list_too_early(client, agency_account_data):
    query_params = "?fiscal_year=2007"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_federal_account_list_future(client, agency_account_data):
    query_params = "?fiscal_year=" + str(current_fiscal_year() + 1)
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_federal_account_list_bad_sort(client, agency_account_data):
    query_params = "?sort=not valid"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_federal_account_list_bad_order(client, agency_account_data):
    query_params = "?order=not valid"
    resp = client.get(url.format(code="007", query_params=query_params))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_federal_account_list_sort_by_name(client, agency_account_data, helpers):
    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=asc&sort=name"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=desc&sort=name"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_list_sort_by_obligated_amount(client, agency_account_data, helpers):
    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=asc&sort=obligated_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=desc&sort=obligated_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_list_sort_by_gross_outlay_amount(client, agency_account_data, helpers):
    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=asc&sort=gross_outlay_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&order=desc&sort=gross_outlay_amount"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 3,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_list_search(client, agency_account_data, helpers):
    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&filter=FA 3"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&filter=TA 5"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
            "limit": 10,
        },
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_federal_account_list_pagination(client, agency_account_data, helpers):
    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&limit=2&page=1"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": True,
            "hasPrevious": False,
            "next": 2,
            "page": 1,
            "previous": None,
            "limit": 2,
            "total": 3,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "FA 1",
                "code": "001-0000",
                "obligated_amount": 111.0,
                "children": [
                    {
                        "gross_outlay_amount": 11100000.0,
                        "name": "TA 1",
                        "code": "001-X-0000-000",
                        "obligated_amount": 111.0,
                    }
                ],
            },
            {
                "gross_outlay_amount": 100000.0,
                "name": "FA 3",
                "code": "003-0000",
                "obligated_amount": 100.0,
                "children": [
                    {
                        "gross_outlay_amount": 100000.0,
                        "name": "TA 6",
                        "code": "003-2017/2018-0000-000",
                        "obligated_amount": 100.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    query_params = f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}&limit=2&page=2"
    resp = client.get(url.format(code="007", query_params=query_params))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "toptier_code": "007",
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": True,
            "next": None,
            "page": 2,
            "previous": 1,
            "total": 3,
            "limit": 2,
        },
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "FA 2",
                "code": "002-0000",
                "obligated_amount": 10.0,
                "children": [
                    {
                        "gross_outlay_amount": 1000000.0,
                        "name": "TA 5",
                        "code": "002-2008/2009-0000-000",
                        "obligated_amount": 10.0,
                    }
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_latest_submission_per_agency_used(client, agency_account_data, helpers):
    # Test File B record is in latest submission for agency
    query_params = f"?fiscal_year=2016"
    resp = client.get(url.format(code="011", query_params=query_params))

    expected_result = {
        "fiscal_year": 2016,
        "results": [
            {
                "children": [
                    {
                        "code": "005-X-0000-000",
                        "gross_outlay_amount": 7000.0,
                        "name": "TA 7",
                        "obligated_amount": 700.0,
                    }
                ],
                "code": "005-0000",
                "gross_outlay_amount": 7000.0,
                "name": "FA 5",
                "obligated_amount": 700.0,
            }
        ],
        "toptier_code": "011",
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "messages": [
            "Account data powering this endpoint were first collected in "
            "FY2017 Q2 under the DATA Act; as such, there are no data "
            "available for prior fiscal years."
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # Test agency submitted recently, but most recent File B record is in prior submission
    query_params = f"?fiscal_year=2017"
    resp = client.get(url.format(code="011", query_params=query_params))

    expected_result = {
        "fiscal_year": 2017,
        "results": [],
        "toptier_code": "011",
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "messages": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
