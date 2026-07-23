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
        "/api/v2/search/spending_by_category/county",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/county",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "county",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 550005.0, "code": "001", "id": None, "name": "CHARLESTON", "total_outlays": None},
            {"amount": 5500.0, "code": "005", "id": None, "name": "TEST NAME", "total_outlays": None},
            {"amount": 50.0, "code": "005", "id": None, "name": "TEST NAME", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_of_empty_list(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/county",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]}}),
    )
    expected_response = {
        "category": "county",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response

def test_category_dataclass_subaward_caching(client, monkeypatch, awards_and_transactions,
                                                elasticsearch_subaward_index, elasticsearch_transaction_index):
    # Tests that the Category dataclass is not being cached incorrectly
    # Originally would happen when updated by subaward spending levels
    # tested by making a spending_level subawards request and then requesting on transaction level
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    sub_resp = client.post(
        "/api/v2/search/spending_by_category/county",
        content_type="application/json",
        data=json.dumps({
            "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}, 
            "spending_level": "subawards"       
        }),
    )
    
    assert sub_resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert sub_resp.json().get("spending_level") == "subawards"
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    trn_resp = client.post(
        "/api/v2/search/spending_by_category/county",
        content_type="application/json",
        data=json.dumps({
            "filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}, 
            "spending_level": "transactions"       
        }),
    )

    expected_response = {
        "category": "county",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 550005.0, "code": "001", "id": None, "name": "CHARLESTON", "total_outlays": None},
            {"amount": 5500.0, "code": "005", "id": None, "name": "TEST NAME", "total_outlays": None},
            {"amount": 50.0, "code": "005", "id": None, "name": "TEST NAME", "total_outlays": None},
        ],
        "messages": _expected_messages(),
        "spending_level": "transactions",
    }
    assert trn_resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert trn_resp.json() == expected_response
