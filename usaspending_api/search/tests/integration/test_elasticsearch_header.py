import json
import pytest

from rest_framework import status
from django.conf import settings

from usaspending_api.common.experimental_api_flags import ELASTICSEARCH_HEADER_VALUE, EXPERIMENTAL_API_HEADER


@pytest.mark.django_db
def test_elasticsearch_headers(client, monkeypatch, elasticsearch_transaction_index):
    elasticsearch_http_header_helper(client, monkeypatch, elasticsearch_transaction_index, "awarding_agency")
    elasticsearch_http_header_helper(client, monkeypatch, elasticsearch_transaction_index, "awarding_subagency")
    elasticsearch_http_header_helper(client, monkeypatch, elasticsearch_transaction_index, "funding_agency")
    elasticsearch_http_header_helper(client, monkeypatch, elasticsearch_transaction_index, "funding_subagency")


def elasticsearch_http_header_helper(client, monkeypatch, elasticsearch_transaction_index, endpoint_name):
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
        f"/api/v2/search/spending_by_category/{endpoint_name}",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 1, "Expected one logging statement"
    assert (
        logging_statements[0]
        == f"Using experimental Elasticsearch functionality for 'spending_by_category/{endpoint_name}'"
    ), "Expected a different logging statement"

    # Logging statement is NOT triggered for Prime Awards when Header is NOT present
    logging_statements.clear()
    resp = client.post(
        f"/api/v2/search/spending_by_category/{endpoint_name}",
        content_type="application/json",
        data=json.dumps({"filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Prime Awards without the Header"

    # Logging statement is NOT triggered for Sub Awards when Header is present
    logging_statements.clear()
    resp = client.post(
        f"/api/v2/search/spending_by_category/{endpoint_name}",
        content_type="application/json",
        data=json.dumps({"subawards": True, "filters": {"keywords": ["test", "testing"]}}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Sub Awards with the Header"

    # Logging statement is NOT triggered for Sub Awards when Header is NOT present
    logging_statements.clear()
    resp = client.post(
        f"/api/v2/search/spending_by_category/{endpoint_name}",
        content_type="application/json",
        data=json.dumps({"subawards": True, "filters": {"keywords": ["test", "testing"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(logging_statements) == 0, "Expected zero logging statements for Sub Awards without the Header"
