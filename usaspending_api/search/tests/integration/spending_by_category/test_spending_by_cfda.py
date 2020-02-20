import json

from rest_framework import status

from usaspending_api.common.experimental_api_flags import ELASTICSEARCH_HEADER_VALUE, EXPERIMENTAL_API_HEADER
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.integration.spending_by_category.utilities import setup_elasticsearch_test


"""
As of 02/04/2020 these are intended for the experimental Elasticsearch functionality that lives alongside the Postgres
implementation. These tests verify that ES performs as expected, but that it also respects the header put in place
to trigger the experimental functionality. When ES for spending_by_category is used as the primary implementation for
the endpoint these tests should be updated to reflect the change.
"""


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, basic_agencies):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    logging_statements = []
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    resp = client.post(
        "/api/v2/search/spending_by_category/cfda",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"


def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, cfda_award):

    logging_statements = []
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    resp = client.post(
        "/api/v2/search/spending_by_category/cfda",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2009-10-01", "end_date": "2024-09-30"}]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "category": "cfda",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 10.0, "code": "102.0", "id": 102, "name": "One hundred and two"},
            {"amount": 10.0, "code": "101.0", "id": 101, "name": "One hundred and one"},
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response


def test_aggregation_across_awards(client, monkeypatch, elasticsearch_transaction_index, single_cfda_2_awards):

    logging_statements = []
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    resp = client.post(
        "/api/v2/search/spending_by_category/cfda",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2009-10-01", "end_date": "2024-09-30"}]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "category": "cfda",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [{"amount": 20.0, "code": "101.0", "id": 101, "name": "One hundred and one"}],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response
