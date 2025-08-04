import itertools

import pytest
from model_bakery import baker

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.v2.elasticsearch_helper import (
    spending_by_transaction_count,
    get_download_ids,
)
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize
from usaspending_api.search.filters.elasticsearch.filter import QueryType


@pytest.fixture
def transaction_type_data(db):
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_description="pop tart",
        piid="0001",
    )
    baker.make("search.AwardSearch", award_id=1, latest_transaction_id=1, is_fpds=True, type="A", piid="0001")

    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        action_date="2010-10-01",
        is_fpds=False,
        type="02",
        transaction_description="pop tart",
        fain="0002",
    )
    baker.make("search.AwardSearch", award_id=2, latest_transaction_id=2, is_fpds=False, type="02", fain="0002")

    baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=3,
        action_date="2010-10-01",
        is_fpds=False,
        type="11",
        transaction_description="pop tart",
        fain="0003",
    )
    baker.make("search.AwardSearch", award_id=3, latest_transaction_id=3, is_fpds=False, type="11", fain="0003")

    baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=4,
        action_date="2010-10-01",
        is_fpds=False,
        type="06",
        transaction_description="pop tart",
        fain="0004",
    )
    baker.make("search.AwardSearch", award_id=4, latest_transaction_id=4, is_fpds=False, type="06", fain="0004")

    baker.make(
        "search.TransactionSearch",
        transaction_id=5,
        award_id=5,
        action_date="2010-10-01",
        is_fpds=False,
        type="07",
        transaction_description="pop tart",
        fain="0006",
    )
    baker.make("search.AwardSearch", award_id=5, latest_transaction_id=5, is_fpds=False, type="07", fain="0005")

    baker.make(
        "search.TransactionSearch",
        transaction_id=6,
        award_id=6,
        action_date="2010-10-01",
        is_fpds=True,
        type="IDV_A",
        transaction_description="pop tart",
        piid="0006",
    )
    baker.make("search.AwardSearch", award_id=6, latest_transaction_id=6, is_fpds=True, type="IDV_A", piid="0006")


@pytest.mark.django_db
def test_spending_by_transaction_count(monkeypatch, transaction_type_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    request_data = {"filters": {"keywords": ["pop tart"]}}
    request_data["filters"]["keyword_search"] = [es_minimal_sanitize(x) for x in request_data["filters"]["keywords"]]
    request_data["filters"].pop("keywords")
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    filter_query = query_with_filters.generate_elasticsearch_query(request_data["filters"])
    search = TransactionSearch().filter(filter_query)
    results = spending_by_transaction_count(search)
    expected_results = {"contracts": 1, "grants": 1, "idvs": 1, "loans": 1, "direct_payments": 1, "other": 1}
    assert results == expected_results


@pytest.mark.django_db
def test_get_download_ids(monkeypatch, transaction_type_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    results = get_download_ids(["pop tart"], "transaction_id")
    transaction_ids = list(itertools.chain.from_iterable(results))
    expected_results = [1, 2, 3, 4, 5, 6]

    assert transaction_ids == expected_results
