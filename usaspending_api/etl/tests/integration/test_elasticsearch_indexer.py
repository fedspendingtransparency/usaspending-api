import pytest

from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from django.test import override_settings
from elasticsearch import Elasticsearch
from model_mommy import mommy

from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.conftest_helpers import TestElasticSearchIndex
from usaspending_api.etl.elasticsearch_loader_helpers import set_final_index_config
from usaspending_api.awards.models import Award, TransactionFABS, TransactionFPDS, TransactionNormalized
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import ES_AWARDS_UNIQUE_KEY_FIELD
from usaspending_api.etl.management.commands.elasticsearch_indexer import (
    Command as ElasticsearchIndexerCommand,
    parse_cli_args,
)
from usaspending_api.etl.elasticsearch_loader_helpers import (
    Controller,
    delete_awards,
    delete_transactions,
)
from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import (
    _check_awards_for_deletes,
    _lookup_deleted_award_keys,
    delete_docs_by_unique_key,
)


@pytest.fixture
def award_data_fixture(db):
    fpds_unique_key = "fpds_transaction_id_1".upper()  # our ETL UPPERs all these when brought from Broker
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_unique_id=fpds_unique_key,
    )
    mommy.make(
        "awards.TransactionFPDS",
        detached_award_proc_unique=fpds_unique_key,
        transaction_id=1,
        legal_entity_zip5="abcde",
        piid="IND12PB00323",
        legal_entity_county_code="059",
        legal_entity_state_code="VA",
        legal_entity_congressional="11",
        legal_entity_country_code="USA",
        place_of_performance_state="VA",
        place_of_performance_congr="11",
        place_of_perform_country_c="USA",
        naics="331122",
        product_or_service_code="1510",
        type_set_aside="8AN",
        type_of_contract_pricing="2",
        extent_competed="F",
    )

    fabs_unique_key = "fabs_transaction_id_2".upper()  # our ETL UPPERs all these when brought from Broker
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        action_date="2016-10-01",
        is_fpds=False,
        type="02",
        transaction_unique_id=fabs_unique_key,
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction_id=2,
        fain="P063P100612",
        cfda_number="84.063",
        afa_generated_unique=fabs_unique_key,
    )

    mommy.make("references.ToptierAgency", toptier_agency_id=1, name="Department of Transportation")
    mommy.make("references.SubtierAgency", subtier_agency_id=1, name="Department of Transportation")
    mommy.make("references.Agency", id=1, toptier_agency_id=1, subtier_agency_id=1)
    mommy.make(
        "awards.Award",
        id=1,
        generated_unique_award_id="CONT_AWD_IND12PB00323",
        latest_transaction_id=1,
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        description="pop tarts and assorted cereals",
        total_obligation=500000.00,
        date_signed="2010-10-1",
        awarding_agency_id=1,
        funding_agency_id=1,
        update_date="2012-05-19",
    )
    mommy.make(
        "awards.Award",
        id=2,
        generated_unique_award_id="ASST_NON_P063P100612",
        latest_transaction_id=2,
        is_fpds=False,
        type="02",
        fain="P063P100612",
        total_obligation=1000000.00,
        date_signed="2016-10-1",
        update_date="2014-07-21",
    )
    mommy.make("accounts.FederalAccount", id=1)
    mommy.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=1,
        agency_id="097",
        main_account_code="4930",
        federal_account_id=1,
    )

    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=1, treasury_account_id=1)


def mock_execute_sql(sql, results, verbosity=None):
    """SQL method is being mocked here since the `execute_sql_statement` used
    doesn't use the same DB connection to avoid multiprocessing errors
    """
    return execute_sql_to_ordered_dictionary(sql)


def test_create_and_load_new_award_index(award_data_fixture, elasticsearch_award_index, monkeypatch):
    """Test the ``elasticsearch_loader`` django management command to create a new awards index and load it
    with data from the DB
    """
    client = elasticsearch_award_index.client  # type: Elasticsearch

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_award_index.index_name)
    original_db_awards_count = Award.objects.count()

    # Inject ETL arg into config for this run, which loads a newly created index
    elasticsearch_award_index.etl_config["create_new_index"] = True
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index)

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    loader = Controller(es_etl_config)
    assert loader.__class__.__name__ == "Controller"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    # Along with other things, this will refresh the index, to surface loaded docs
    set_final_index_config(client, elasticsearch_award_index.index_name)

    assert client.indices.exists(elasticsearch_award_index.index_name)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count


def test_create_and_load_new_transaction_index(award_data_fixture, elasticsearch_transaction_index, monkeypatch):
    """Test the ``elasticsearch_loader`` django management command to create a new transactions index and load it
    with data from the DB
    """
    client = elasticsearch_transaction_index.client  # type: Elasticsearch

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_transaction_index.index_name)
    original_db_tx_count = TransactionNormalized.objects.count()

    # Inject ETL arg into config for this run, which loads a newly created index
    elasticsearch_transaction_index.etl_config["create_new_index"] = True
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_transaction_index)

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    loader = Controller(es_etl_config)
    assert loader.__class__.__name__ == "Controller"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    # Along with other things, this will refresh the index, to surface loaded docs
    set_final_index_config(client, elasticsearch_transaction_index.index_name)

    assert client.indices.exists(elasticsearch_transaction_index.index_name)
    es_award_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_award_docs == original_db_tx_count


def test_incremental_load_into_award_index(award_data_fixture, elasticsearch_award_index, monkeypatch):
    """Test the ``elasticsearch_loader`` django management command to incrementally load updated data into the awards ES
    index from the DB, overwriting the doc that was already there
    """
    original_db_awards_count = Award.objects.count()
    elasticsearch_award_index.update_index()
    client = elasticsearch_award_index.client  # type: Elasticsearch
    assert client.indices.exists(elasticsearch_award_index.index_name)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count

    # Inject ETL arg into config for this run, to suppress processing deletes. Test incremental load only
    elasticsearch_award_index.etl_config["process_deletes"] = False
    elasticsearch_award_index.etl_config["start_datetime"] = datetime.now(timezone.utc)
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index)

    # Now modify one of the DB objects
    awd = Award.objects.first()  # type: Award
    awd.total_obligation = 9999
    awd.save()

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    ensure_view_exists(es_etl_config["sql_view"], force=True)
    loader = Controller(es_etl_config)
    assert loader.__class__.__name__ == "Controller"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    client.indices.refresh(elasticsearch_award_index.index_name)

    assert client.indices.exists(elasticsearch_award_index.index_name)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count
    es_awards = client.search(index=elasticsearch_award_index.index_name)
    updated_award = [a for a in es_awards["hits"]["hits"] if a["_source"]["award_id"] == awd.id][0]
    assert int(updated_award["_source"]["total_obligation"]) == 9999


def test_incremental_load_into_transaction_index(award_data_fixture, elasticsearch_transaction_index, monkeypatch):
    """Test the ``elasticsearch_loader`` django management command to incrementally load updated data into
    the transactions ES index from the DB, overwriting the doc that was already there
    """
    original_db_txs_count = TransactionNormalized.objects.count()
    elasticsearch_transaction_index.update_index()
    client = elasticsearch_transaction_index.client  # type: Elasticsearch
    assert client.indices.exists(elasticsearch_transaction_index.index_name)
    es_tx_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_tx_docs == original_db_txs_count

    # Inject ETL arg into config for this run, to suppress processing deletes. Test incremental load only
    elasticsearch_transaction_index.etl_config["process_deletes"] = False
    elasticsearch_transaction_index.etl_config["start_datetime"] = datetime.now(timezone.utc)
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_transaction_index)

    # Now modify one of the DB objects
    tx = TransactionNormalized.objects.first()  # type: TransactionNormalized
    tx.federal_action_obligation = 9999
    tx.save()

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    ensure_view_exists(es_etl_config["sql_view"], force=True)
    loader = Controller(es_etl_config)
    assert loader.__class__.__name__ == "Controller"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    client.indices.refresh(elasticsearch_transaction_index.index_name)

    assert client.indices.exists(elasticsearch_transaction_index.index_name)
    es_tx_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_tx_docs == original_db_txs_count
    es_txs = client.search(index=elasticsearch_transaction_index.index_name)
    updated_tx = [t for t in es_txs["hits"]["hits"] if t["_source"]["transaction_id"] == tx.id][0]
    assert int(updated_tx["_source"]["federal_action_obligation"]) == 9999


def test__lookup_deleted_award_keys(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()
    client = elasticsearch_transaction_index.client
    ids = _lookup_deleted_award_keys(
        client,
        "afa_generated_unique",
        ["FABS_TRANSACTION_ID_2"],
        elasticsearch_transaction_index.etl_config,
        index=elasticsearch_transaction_index.index_name,
    )
    assert ids == ["ASST_NON_P063P100612"]


def test__lookup_deleted_award_keys_multiple_chunks(award_data_fixture, elasticsearch_transaction_index):
    """Test that when the lookup requires multiple iterative calls to ES to match values, it still gets all of them"""
    elasticsearch_transaction_index.update_index()
    client = elasticsearch_transaction_index.client
    ids = _lookup_deleted_award_keys(
        client,
        "generated_unique_transaction_id",
        ["CONT_TX_FPDS_TRANSACTION_ID_1", "ASST_TX_FABS_TRANSACTION_ID_2"],
        elasticsearch_transaction_index.etl_config,
        index=elasticsearch_transaction_index.index_name,
        lookup_chunk_size=1,
    )
    assert "CONT_AWD_IND12PB00323" in ids and "ASST_NON_P063P100612" in ids
    assert len(ids) == 2


def test__lookup_deleted_award_keys_by_int(award_data_fixture, elasticsearch_award_index):
    """Looks up awards off of an awards index using award_id as the lookup_key field"""
    elasticsearch_award_index.update_index()
    client = elasticsearch_award_index.client
    ids = _lookup_deleted_award_keys(
        client, "award_id", [1], elasticsearch_award_index.etl_config, index=elasticsearch_award_index.index_name
    )
    assert ids == ["CONT_AWD_IND12PB00323"]


def test_delete_docs_by_unique_key_exceed_max_terms(award_data_fixture, elasticsearch_award_index):
    """Verify we restrict attempting to delete more than allowed in a terms query"""
    elasticsearch_award_index.update_index()
    with pytest.raises(RuntimeError) as exc_info:
        delete_docs_by_unique_key(
            elasticsearch_award_index.client,
            ES_AWARDS_UNIQUE_KEY_FIELD,
            list(map(str, range(0, 70000))),
            "test delete",
            elasticsearch_award_index.index_name,
            delete_chunk_size=70000,
        )
        assert "greater than 65536" in str(exc_info.value)


def test_delete_docs_by_unique_key_exceed_max_results_window(award_data_fixture, elasticsearch_award_index):
    """Verify that trying to delete more records at once than the index max_results_window will error-out"""
    fake_max_results_window = 1
    with override_settings(ES_AWARDS_MAX_RESULT_WINDOW=fake_max_results_window):
        elasticsearch_award_index.update_index()
        assert elasticsearch_award_index.etl_config["max_query_size"] == fake_max_results_window

        with pytest.raises(Exception) as exc_info:
            delete_docs_by_unique_key(
                elasticsearch_award_index.client,
                "award_id",
                [1, 2],
                "test delete",
                elasticsearch_award_index.index_name,
                delete_chunk_size=10,
            )
            assert "Batch size is too large" in str(exc_info.value)
            assert "controlled by the [index.max_result_window] index level setting" in str(exc_info.value)


def test__check_awards_for_deletes(award_data_fixture, monkeypatch, db):
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data.execute_sql_statement", mock_execute_sql
    )
    id_list = ["CONT_AWD_IND12PB00323"]
    awards = _check_awards_for_deletes(id_list)
    assert awards == []

    id_list = ["CONT_AWD_WHATEVER", "CONT_AWD_IND12PB00323"]
    awards = _check_awards_for_deletes(id_list)
    assert awards == [OrderedDict([("generated_unique_award_id", "CONT_AWD_WHATEVER")])]


def test_delete_awards(award_data_fixture, elasticsearch_transaction_index, elasticsearch_award_index, monkeypatch, db):
    """Transactions that are logged for delete, that are in the transaction ES index, and their parent awards are NOT
    in the DB ... are deleted from the ES awards index

    The current delete approach intakes transaction IDs from the S3 delete log file, looks them up in the transaction
    index, gets their unique award key from each transaction, and checks if they were deleted from the DB. If so, THEN
    it deletes them from the awards ES index.
    """
    elasticsearch_transaction_index.update_index()
    elasticsearch_award_index.update_index()
    delete_time = datetime.now(timezone.utc)
    last_load_time = delete_time - timedelta(hours=12)

    fpds_keys = [
        "CONT_TX_" + key.upper()
        for key in TransactionNormalized.objects.filter(is_fpds=True).values_list("transaction_unique_id", flat=True)
    ]
    fabs_keys = [
        "ASST_TX_" + key.upper()
        for key in TransactionNormalized.objects.filter(is_fpds=False).values_list("transaction_unique_id", flat=True)
    ]
    deleted_tx = {key: {"timestamp": delete_time} for key in fpds_keys + fabs_keys}
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg: deleted_tx,
    )

    original_db_awards_count = Award.objects.count()
    # Simulate an awards ETL deleting the transactions and awards from the DB.
    TransactionNormalized.objects.all().delete()
    TransactionFPDS.objects.all().delete()
    TransactionFABS.objects.all().delete()
    Award.objects.all().delete()

    client = elasticsearch_award_index.client  # type: Elasticsearch
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count
    elasticsearch_award_index.etl_config["start_datetime"] = last_load_time
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index)
    # Must use mock sql function to share test DB conn+transaction in ETL code
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data.execute_sql_statement", mock_execute_sql
    )
    delete_awards(client, es_etl_config)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == 0


def test_delete_awards_zero_for_unmatched_transactions(
    award_data_fixture, elasticsearch_transaction_index, elasticsearch_award_index, monkeypatch, db
):
    """No awards deleted from the index if their transactions are not found in the transaction index

    If the logged deleted transactions are not found in the transaction index, then there is no way to fetch unique
    award keys and remove those from the ES index.
    """
    elasticsearch_transaction_index.update_index()
    elasticsearch_award_index.update_index()
    delete_time = datetime.now(timezone.utc)
    last_load_time = delete_time - timedelta(hours=12)

    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg: {
            "unmatchable_tx_key1": {"timestamp": delete_time},
            "unmatchable_tx_key2": {"timestamp": delete_time},
            "unmatchable_tx_key3": {"timestamp": delete_time},
        },
    )

    client = elasticsearch_award_index.client  # type: Elasticsearch
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == Award.objects.count()
    elasticsearch_award_index.etl_config["start_datetime"] = last_load_time
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index)
    delete_count = delete_awards(client, es_etl_config)
    assert delete_count == 0
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == Award.objects.count()


def test_delete_one_assistance_award(
    award_data_fixture, elasticsearch_transaction_index, elasticsearch_award_index, monkeypatch, db
):
    """Ensure that transactions not logged for delete don't cause their parent awards to get deleted

    Similar to test that logs all transactions as deleted and deletes all ES awards, but just picking 1 to delete
    """
    elasticsearch_transaction_index.update_index()
    elasticsearch_award_index.update_index()
    delete_time = datetime.now(timezone.utc)
    last_load_time = delete_time - timedelta(hours=12)

    # Get FABS transaction with the lowest ID. This ONE will be deleted.
    tx = TransactionNormalized.objects.filter(is_fpds=False).order_by("pk").first()  # type: TransactionNormalized
    fabs_key = "ASST_TX_" + tx.transaction_unique_id.upper()
    deleted_tx = {fabs_key: {"timestamp": delete_time}}
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg: deleted_tx,
    )

    original_db_awards_count = Award.objects.count()
    # Simulate an awards ETL deleting the transactions and awards from the DB.
    TransactionFABS.objects.filter(pk=tx.pk).delete()
    tx.award.delete()
    tx.delete()

    client = elasticsearch_award_index.client  # type: Elasticsearch
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count
    elasticsearch_award_index.etl_config["start_datetime"] = last_load_time
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index)
    # Must use mock sql function to share test DB conn+transaction in ETL code
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data.execute_sql_statement", mock_execute_sql
    )
    delete_count = delete_awards(client, es_etl_config)
    assert delete_count == 1
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count - 1


def test_delete_one_assistance_transaction(award_data_fixture, elasticsearch_transaction_index, monkeypatch, db):
    """Ensure that transactions not logged for delete don't get deleted but those logged for delete do"""
    elasticsearch_transaction_index.update_index()
    delete_time = datetime.now(timezone.utc)
    last_load_time = delete_time - timedelta(hours=12)

    # Get FABS transaction with the lowest ID. This ONE will be deleted.
    tx = TransactionNormalized.objects.filter(is_fpds=False).order_by("pk").first()  # type: TransactionNormalized
    fabs_key = "ASST_TX_" + tx.transaction_unique_id.upper()
    deleted_tx = {fabs_key: {"timestamp": delete_time}}
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg: deleted_tx,
    )

    original_db_tx_count = TransactionNormalized.objects.count()
    # Simulate an awards ETL deleting the transaction and award from the DB.
    TransactionFABS.objects.filter(pk=tx.pk).delete()
    tx.award.delete()
    tx.delete()

    client = elasticsearch_transaction_index.client  # type: Elasticsearch
    es_award_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_award_docs == original_db_tx_count
    elasticsearch_transaction_index.etl_config["start_datetime"] = last_load_time
    es_etl_config = _process_es_etl_test_config(client, elasticsearch_transaction_index)
    # Must use mock sql function to share test DB conn+transaction in ETL code
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data.execute_sql_statement", mock_execute_sql
    )
    delete_count = delete_transactions(client, es_etl_config)
    assert delete_count == 1
    es_award_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_award_docs == original_db_tx_count - 1


def _process_es_etl_test_config(client: Elasticsearch, test_es_index: TestElasticSearchIndex):
    """Use the Django mgmt cmd to extract args with default values, then update those with test ETL config values"""
    cmd = ElasticsearchIndexerCommand()
    cmd_name = cmd.__module__.split(".")[-1]  # should give "elasticsearch_indexer" unless name changed
    parser = cmd.create_parser("", cmd_name)
    # Changes dict of arg k-v pairs into a flat list of ordered ["k1", "v1", "k2", "v2" ...] items
    list_of_arg_kvs = [["--" + k.replace("_", "-"), str(v)] for k, v in test_es_index.etl_config.items()]
    test_args = [arg_item for kvpair in list_of_arg_kvs for arg_item in kvpair]
    cli_args, _ = parser.parse_known_args(args=test_args)  # parse the known args programmatically
    cli_opts = {**vars(cli_args), **test_es_index.etl_config}  # update defaults with test config
    es_etl_config = parse_cli_args(cli_opts, client)  # use command's config parser for final config for testing ETL
    return es_etl_config
