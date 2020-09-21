import itertools

import pytest

from model_mommy import mommy

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.search.v2.elasticsearch_helper import (
    spending_by_transaction_count,
    get_download_ids,
    es_minimal_sanitize,
    swap_keys,
)
from usaspending_api.search.v2.es_sanitization import es_sanitize


@pytest.fixture
def transaction_type_data(db):
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        description="pop tart",
    )
    mommy.make("awards.TransactionFPDS", transaction_id=1, piid="0001")
    mommy.make("awards.Award", id=1, latest_transaction_id=1, is_fpds=True, type="A", piid="0001")

    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        action_date="2010-10-01",
        is_fpds=False,
        type="02",
        description="pop tart",
    )
    mommy.make("awards.TransactionFABS", transaction_id=2, fain="0002")
    mommy.make("awards.Award", id=2, latest_transaction_id=2, is_fpds=False, type="02", fain="0002")

    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        award_id=3,
        action_date="2010-10-01",
        is_fpds=False,
        type="11",
        description="pop tart",
    )
    mommy.make("awards.TransactionFABS", transaction_id=3, fain="0003")
    mommy.make("awards.Award", id=3, latest_transaction_id=3, is_fpds=False, type="11", fain="0003")

    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        award_id=4,
        action_date="2010-10-01",
        is_fpds=False,
        type="06",
        description="pop tart",
    )
    mommy.make("awards.TransactionFABS", transaction_id=4, fain="0004")
    mommy.make("awards.Award", id=4, latest_transaction_id=4, is_fpds=False, type="06", fain="0004")

    mommy.make(
        "awards.TransactionNormalized",
        id=5,
        award_id=5,
        action_date="2010-10-01",
        is_fpds=False,
        type="07",
        description="pop tart",
    )
    mommy.make("awards.TransactionFABS", transaction_id=5, fain="0006")
    mommy.make("awards.Award", id=5, latest_transaction_id=5, is_fpds=False, type="07", fain="0005")

    mommy.make(
        "awards.TransactionNormalized",
        id=6,
        award_id=6,
        action_date="2010-10-01",
        is_fpds=True,
        type="IDV_A",
        description="pop tart",
    )
    mommy.make("awards.TransactionFPDS", transaction_id=6, piid="0006")
    mommy.make("awards.Award", id=6, latest_transaction_id=6, is_fpds=True, type="IDV_A", piid="0006")


def test_spending_by_transaction_count(monkeypatch, transaction_type_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    request_data = {"filters": {"keywords": ["pop tart"]}}
    results = spending_by_transaction_count(request_data)
    expected_results = {"contracts": 1, "grants": 1, "idvs": 1, "loans": 1, "direct_payments": 1, "other": 1}
    assert results == expected_results


def test_get_download_ids(monkeypatch, transaction_type_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    results = get_download_ids(["pop tart"], "transaction_id")
    transaction_ids = list(itertools.chain.from_iterable(results))
    expected_results = [1, 2, 3, 4, 5, 6]

    assert transaction_ids == expected_results


def test_es_sanitize():
    test_string = '+|()[]{}?"<>\\'
    processed_string = es_sanitize(test_string)
    assert processed_string == ""
    test_string = "!-^~/&:*"
    processed_string = es_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/\&\:\*"


def test_es_minimal_sanitize():
    test_string = "https://www.localhost:8000/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"https\:\/\/www.localhost\:8000\/"
    test_string = "!-^~/"
    processed_string = es_minimal_sanitize(test_string)
    assert processed_string == r"\!\-\^\~\/"


def test_swap_keys():
    test = {
        "Recipient Name": "recipient_name",
        "Action Date": "action_date",
        "Transaction Amount": "transaction_amount",
    }

    results = swap_keys(test)

    assert results == {
        "recipient_name": "recipient_name",
        "action_date": "action_date",
        "transaction_amount": "transaction_amount",
    }
