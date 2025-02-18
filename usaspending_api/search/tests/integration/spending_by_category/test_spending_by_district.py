import json

import pytest
from rest_framework import status

from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def expected_messages():
    return [
        get_time_period_message(),
        "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
        "See documentation for more information. ",
    ]


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/district",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"


def test_correct_response(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions, expected_messages
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/district",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
    )
    expected_response = {
        "category": "district",
        "spending_level": "transactions",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 500000.0, "code": "90", "id": None, "name": "SC-MULTIPLE DISTRICTS", "total_outlays": None},
            {"amount": 50005.0, "code": "10", "id": None, "name": "SC-10", "total_outlays": None},
            {"amount": 5500.0, "code": "51", "id": None, "name": "WA-51", "total_outlays": None},
            {"amount": 50.0, "code": "51", "id": None, "name": "SC-51", "total_outlays": None},
        ],
        "messages": expected_messages,
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response


def test_correct_response_of_empty_list(
    client, monkeypatch, elasticsearch_transaction_index, awards_and_transactions, expected_messages
):

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    resp = client.post(
        "/api/v2/search/spending_by_category/district",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2008-10-01", "end_date": "2009-09-30"}]}}),
    )
    expected_response = {
        "category": "district",
        "spending_level": "transactions",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [],
        "messages": expected_messages,
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert resp.json() == expected_response
