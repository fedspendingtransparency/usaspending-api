import pytest

from rest_framework import status
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


url = "/api/v2/disaster/agency/spending/"


@pytest.mark.django_db
def test_basic_success(client, disaster_account_data, elasticsearch_award_index, monkeypatch, helpers):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(
        client, url, def_codes=["L", "M", "N", "O", "P"], spending_type="total", sort="description"
    )
    expected_results = [
        {
            "id": 4,
            "code": "009",
            "description": "Agency 009",
            "children": [],
            "award_count": None,
            "obligation": 11000000.0,
            "outlay": 11.0,
            "total_budgetary_resources": 23984723890.78,
        },
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "children": [],
            "award_count": None,
            "obligation": 1000.0,
            "outlay": 10000.0,
            "total_budgetary_resources": 8927431230.12,
        },
        {
            "id": 1,
            "code": "007",
            "description": "Agency 007",
            "children": [],
            "award_count": None,
            "obligation": 0.0,
            "outlay": 0.0,
            "total_budgetary_resources": 0.0,
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L"], spending_type="total")
    expected_results = [
        {
            "id": 1,
            "code": "007",
            "description": "Agency 007",
            "children": [],
            "award_count": None,
            "obligation": 0.0,
            "outlay": 0.0,
            "total_budgetary_resources": 0.0,
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["L", "M", "N", "O", "P"], spending_type="award")
    expected_results = [
        {
            "id": 4,
            "code": "009",
            "description": "Agency 009",
            "children": [],
            "award_count": 3,
            "obligation": 22199998.0,
            "outlay": 200000022.0,
            "total_budgetary_resources": None,
        },
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "children": [],
            "award_count": 2,
            "obligation": 22000.0,
            "outlay": 20000.0,
            "total_budgetary_resources": None,
        },
        {
            "id": 1,
            "code": "007",
            "description": "Agency 007",
            "children": [],
            "award_count": 1,
            "obligation": 222.0,
            "outlay": 0.0,
            "total_budgetary_resources": None,
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_award_type_codes(client, disaster_account_data, elasticsearch_award_index, monkeypatch, helpers):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["A", "07", "02"], def_codes=["L", "M", "N", "O", "P"], spending_type="award",
    )
    expected_results = [
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "award_count": 3,
            "obligation": 21999998.0,
            "outlay": 200000022.0,
            "children": [
                {
                    "code": "2008",
                    "award_count": 2,
                    "description": "Subtier 2008",
                    "id": 2,
                    "obligation": 19999998.0,
                    "outlay": 200000002.0,
                },
                {
                    "code": "1008",
                    "award_count": 1,
                    "description": "Subtier 1008",
                    "id": 2,
                    "obligation": 2000000.0,
                    "outlay": 20.0,
                },
            ],
        },
        {
            "id": 1,
            "code": "007",
            "description": "Agency 007",
            "award_count": 1,
            "obligation": 2000.0,
            "outlay": 20000.0,
            "children": [
                {
                    "id": 1,
                    "code": "1007",
                    "description": "Subtier 1007",
                    "award_count": 1,
                    "obligation": 2000.0,
                    "outlay": 20000.0,
                }
            ],
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["A"], def_codes=["L", "M", "N", "O", "P"], spending_type="award",
    )
    expected_results = [
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "award_count": 1,
            "obligation": 20000000.0,
            "outlay": 2.0,
            "children": [
                {
                    "id": 2,
                    "code": "2008",
                    "description": "Subtier 2008",
                    "award_count": 1,
                    "obligation": 20000000.0,
                    "outlay": 2.0,
                }
            ],
        }
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["02"], def_codes=["L", "M", "N", "O", "P"], spending_type="award",
    )
    expected_results = [
        {
            "id": 2,
            "code": "008",
            "description": "Agency 008",
            "award_count": 2,
            "obligation": 1999998.0,
            "outlay": 200000020.0,
            "children": [
                {
                    "id": 2,
                    "code": "2008",
                    "description": "Subtier 2008",
                    "award_count": 1,
                    "obligation": -2.0,
                    "outlay": 200000000.0,
                },
                {
                    "id": 2,
                    "code": "1008",
                    "description": "Subtier 1008",
                    "award_count": 1,
                    "obligation": 2000000.0,
                    "outlay": 20.0,
                },
            ],
        },
    ]

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(
        client, url, award_type_codes=["IDV_A"], def_codes=["L", "M", "N", "O", "P"], spending_type="award",
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_empty(client, monkeypatch, elasticsearch_award_index, helpers, generic_account_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="total")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_invalid_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"], spending_type="total")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O', 'P']"


@pytest.mark.django_db
def test_invalid_defc_type(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100", spending_type="total")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_missing_defc(client, generic_account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, spending_type="total")
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"


@pytest.mark.django_db
def test_invalid_spending_type(client, monkeypatch, elasticsearch_award_index, generic_account_data, helpers):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="total")
    assert resp.status_code == status.HTTP_200_OK

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="gibberish")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'spending_type' is outside valid values ['total', 'award']"


@pytest.mark.django_db
def test_missing_spending_type(client, monkeypatch, generic_account_data, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'spending_type' is a required field"
