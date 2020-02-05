import json

import pytest
from django.conf import settings
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.experimental_api_flags import ELASTICSEARCH_HEADER_VALUE, EXPERIMENTAL_API_HEADER
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters


@pytest.fixture
def agency_test_data(db):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1002,
        federal_action_obligation=5,
        action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        awarding_agency_id=1003,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2020-01-02",
    )

    mommy.make("references.ToptierAgency", toptier_agency_id=2001, name="Awarding Toptier Agency 1", abbreviation="TA1")
    mommy.make("references.SubtierAgency", subtier_agency_id=3001, name="Awarding Subtier Agency 1", abbreviation="SA1")
    mommy.make("references.ToptierAgency", toptier_agency_id=2003, name="Awarding Toptier Agency 3", abbreviation="TA3")
    mommy.make("references.SubtierAgency", subtier_agency_id=3003, name="Awarding Subtier Agency 3", abbreviation="SA3")
    mommy.make("references.SubtierAgency", subtier_agency_id=3005, name="Awarding Subtier Agency 5", abbreviation="SA5")

    mommy.make("references.ToptierAgency", toptier_agency_id=2002, name="Funding Toptier Agency 2", abbreviation="TA2")
    mommy.make("references.SubtierAgency", subtier_agency_id=3002, name="Funding Subtier Agency 2", abbreviation="SA2")
    mommy.make("references.ToptierAgency", toptier_agency_id=2004, name="Funding Toptier Agency 4", abbreviation="TA4")
    mommy.make("references.SubtierAgency", subtier_agency_id=3004, name="Funding Subtier Agency 4", abbreviation="SA4")
    mommy.make("references.SubtierAgency", subtier_agency_id=3006, name="Funding Subtier Agency 6", abbreviation="SA6")

    mommy.make("references.Agency", id=1001, toptier_agency_id=2001, subtier_agency_id=3001, toptier_flag=True)
    mommy.make("references.Agency", id=1002, toptier_agency_id=2002, subtier_agency_id=3002, toptier_flag=False)
    mommy.make("references.Agency", id=1006, toptier_agency_id=2002, subtier_agency_id=3006, toptier_flag=True)

    mommy.make("references.Agency", id=1003, toptier_agency_id=2003, subtier_agency_id=3003, toptier_flag=False)
    mommy.make("references.Agency", id=1004, toptier_agency_id=2004, subtier_agency_id=3004, toptier_flag=True)
    mommy.make("references.Agency", id=1005, toptier_agency_id=2003, subtier_agency_id=3005, toptier_flag=True)


"""
As of 02/04/2020 these are intended for the experimental Elasticsearch functionality that lives alongside the Postgres
implementation. These tests verify that ES performs as expected, but that it also respects the header put in place
to trigger the experimental functionality. When ES for spending_by_category is used as the primary implementation for
the endpoint these tests should be updated to reflect the change.
"""


@pytest.mark.django_db
def test_elasticsearch_http_header(client, monkeypatch, elasticsearch_transaction_index):
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

    # Logging statement is triggered for Prime Awards when Header is present
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert (
        logging_statements[0]
        == "Using experimental Elasticsearch functionality for 'spending_by_category/awarding_subagency'"
    ), "Expected a different logging statement"

    # Logging statement is NOT triggered for Prime Awards when Header is NOT present
    logging_statements.clear()
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Prime Awards without the Header"

    # Logging statement is NOT triggered for Sub Awards when Header is present
    logging_statements.clear()
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"subawards": True, "filters": {"keywords": ["test", "testing"]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Sub Awards with the Header"

    # Logging statement is NOT triggered for Sub Awards when Header is NOT present
    logging_statements.clear()
    resp = client.post(
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"subawards": True, "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Sub Awards without the Header"


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
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"


@pytest.mark.django_db
def test_correct_response(client, monkeypatch, elasticsearch_transaction_index, agency_test_data):
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
        "/api/v2/search/spending_by_category/awarding_subagency",
        content_type="application/json",
        data=json.dumps({"filters": {"time_period": [{"start_date": "2018-10-01", "end_date": "2020-09-30"}]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    expected_response = {
        "category": "awarding_subagency",
        "limit": 10,
        "page_metadata": {"page": 1, "next": None, "previous": None, "hasNext": False, "hasPrevious": False},
        "results": [
            {"amount": 10.0, "name": "Awarding Subtier Agency 3", "code": "SA3", "id": 1003},
            {"amount": 5.0, "name": "Awarding Subtier Agency 1", "code": "SA1", "id": 1001},
        ],
        "messages": [get_time_period_message()],
    }
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert resp.json() == expected_response
