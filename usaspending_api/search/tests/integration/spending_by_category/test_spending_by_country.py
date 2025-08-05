import json

from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


def _expected_messages():
    expected_messages = [get_time_period_message()]
    expected_messages.append(
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. "
    )
    return expected_messages


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 5555555.0, "code": "USA", "id": None, "name": "UNITED STATES", "total_outlays": None},
            {"amount": 5000000.0, "code": "CAN", "id": None, "name": "CANADA", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]}}),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_with_date_type(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Tests that date type where no records fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2019-12-30", "end_date": "2020-01-02"}]
                }
            }
        ),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests that date type where records fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [{"date_type": "date_signed", "start_date": "2019-12-30", "end_date": "2020-01-16"}]
                }
            }
        ),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 5.0, "code": "USA", "id": None, "name": "UNITED STATES", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_with_new_awards_only(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Tests where no new awards fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-02"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests where records do fall into range
    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-01", "end_date": "2020-01-17"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 5.0, "code": "USA", "id": None, "name": "UNITED STATES", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

    # Tests where records fall into range with date signed but not action date
    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {
                    "time_period": [
                        {"date_type": "new_awards_only", "start_date": "2020-01-04", "end_date": "2020-01-02"}
                    ]
                }
            }
        ),
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
