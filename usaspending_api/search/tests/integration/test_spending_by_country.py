import json

import pytest
from django.conf import settings
from rest_framework import status

from usaspending_api.common.experimental_api_flags import ELASTICSEARCH_HEADER_VALUE, EXPERIMENTAL_API_HEADER
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.integration.spending_by_category.spending_test_fixtures import (
    setup_basic_awards_and_transactions,
    setup_country_data
)

@pytest.fixture
def country_test_data(db):
    setup_basic_awards_and_transactions()
    setup_country_data()


"""
As of 02/04/2020 these are intended for the experimental Elasticsearch functionality that lives alongside the Postgres
implementation. These tests verify that ES performs as expected, but that it also respects the header put in place
to trigger the experimental functionality. When ES for spending_by_category is used as the primary implementation for
the endpoint these tests should be updated to reflect the change.
"""


@pytest.mark.django_db
def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """
    logging_statements = []
    monkeypatch.setattr(
        "usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category.logger.info",
        lambda message: logging_statements.append(message),
    )
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )

    elasticsearch_transaction_index.update_index()

    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"


@pytest.mark.django_db
def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, country_test_data):
    logging_statements = []
    monkeypatch.setattr(
        "usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category.logger.info",
        lambda message: logging_statements.append(message),
    )
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.TransactionSearch._index_name",
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX,
    )

    elasticsearch_transaction_index.update_index()

    resp = client.post(
        "/api/v2/search/spending_by_category/country",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "category": "country",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 10.0, "name": "UNITED STATES", "code": "USA", "id": None},
            {"amount": 5.0, "name": "IRAQ", "code": "IRQ", "id": None},
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response
