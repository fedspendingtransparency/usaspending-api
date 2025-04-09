from datetime import datetime, timedelta

import pytest
from elasticsearch import Elasticsearch
from model_bakery import baker

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import (
    _check_awards_for_pre_fy2008,
    _gather_modified_transactions_pre_fy2008,
    delete_docs_by_unique_key,
)
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def test_data_fixture(db):
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2024-01-15",
        update_date="2024-01-10 17:02:50.223 -0500",
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="TEST_AWARD_2",
        action_date="2020-01-01",
        update_date="2022-01-10 14:02:50.223 -0500",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        transaction_unique_id="TEST_TRANSACTION_1",
        is_fpds=False,
        afa_generated_unique="ABC_123",
        action_date="2024-01-15",
        etl_update_date=datetime.now(),
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        transaction_unique_id="TEST_TRANSACTION_2",
        is_fpds=True,
        detached_award_proc_unique="DEF_456",
        action_date="2020-01-01",
        etl_update_date=datetime.now() - timedelta(days=1),
    )


@pytest.mark.django_db(transaction=True)
def test_find_modified_awards_before_fy2008(test_data_fixture):
    """Test that we can find any awards that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01), but have
    have been recently updated to have an `action_date` before FY2008 now.
    """

    # Modify the existing DB award to now have an `action_date` before 2007-10-01
    delete_window_start = datetime.utcnow() - timedelta(days=1)
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2004-01-15",
        update_date=datetime.now(),
    )

    recently_modified_awards_before_fy2008 = _check_awards_for_pre_fy2008(
        config={"verbose": False}, delete_window_start=delete_window_start
    )

    assert len(recently_modified_awards_before_fy2008) == 1


@pytest.mark.django_db(transaction=True)
def test_find_modified_transactions_before_fy2008(test_data_fixture):
    """Test that we can find any transactions that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01), but have
    have been recently updated to have an `action_date` before FY2008 now.
    """
    delete_window_start = datetime.now() - timedelta(days=1)

    # Modify the existing DB transactions to now have an `action_date` before 2007-10-01
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        transaction_unique_id="TEST_TRANSACTION_2",
        is_fpds=True,
        detached_award_proc_unique="DEF_456",
        action_date="2004-01-01",
        etl_update_date=datetime.now(),
    )

    recently_modified_awards_before_fy2008 = _gather_modified_transactions_pre_fy2008(
        config={"process_deletes": True, "verbose": False},
        delete_window_start=delete_window_start,
    )

    assert len(recently_modified_awards_before_fy2008) == 1


@pytest.mark.django_db(transaction=True)
def test_delete_modified_awards_before_fy2008(elasticsearch_award_index, test_data_fixture, monkeypatch):
    """Test that we can find any awards that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01), but have
    have been recently updated to have an `action_date` before FY2008 now and delete them from Elasticsearch.
    """

    client: Elasticsearch = elasticsearch_award_index.client

    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)
    original_es_count = client.count(index=elasticsearch_award_index.index_name)["count"]

    # We should have both awards at this point, because both have a valid action_date
    assert original_es_count == 2

    # Modify the existing DB award to now have an `action_date` before 2007-10-01
    delete_window_start = datetime.now() - timedelta(days=1)
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        action_date="2004-01-15",
        update_date=datetime.now(),
    )

    modified_awards_to_delete = _check_awards_for_pre_fy2008(
        config={"verbose": False},
        delete_window_start=delete_window_start,
    )
    number_of_deleted_awards = delete_docs_by_unique_key(
        client=client,
        key="generated_unique_award_id",
        value_list=modified_awards_to_delete,
        task_id="task1",
        index=elasticsearch_award_index.index_name,
    )
    assert number_of_deleted_awards == 1

    new_es_count = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert new_es_count == 1


@pytest.mark.django_db(transaction=True)
def test_delete_modified_transactions_before_fy2008(elasticsearch_transaction_index, test_data_fixture, monkeypatch):
    """Test that we can find any transactions that PREVIOUSLY had an `action_date` on or after FY2008 (2007-10-01),
    but have have been recently updated to have an `action_date` before FY2008 now and delete them from Elasticsearch.
    """

    client: Elasticsearch = elasticsearch_transaction_index.client

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)
    original_es_count = client.count(index=elasticsearch_transaction_index.index_name)["count"]

    # We should have both transactions at this point, because both have a valid action_date
    assert original_es_count == 2

    # Modify the existing DB transaction to now have an `action_date` before 2007-10-01
    delete_window_start = datetime.now() - timedelta(days=1)
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        transaction_unique_id="TEST_TRANSACTION_1",
        is_fpds=False,
        afa_generated_unique="ABC_123",
        action_date="2004-01-15",
        etl_update_date=datetime.now(),
    )

    modified_transactions_to_delete = _gather_modified_transactions_pre_fy2008(
        config={"process_deletes": True, "verbose": False}, delete_window_start=delete_window_start
    )
    number_of_deleted_transactions = delete_docs_by_unique_key(
        client=client,
        key="generated_unique_transaction_id",
        value_list=modified_transactions_to_delete,
        task_id="task1",
        index=elasticsearch_transaction_index.index_name,
    )
    assert number_of_deleted_transactions == 1

    new_es_count = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert new_es_count == 1
