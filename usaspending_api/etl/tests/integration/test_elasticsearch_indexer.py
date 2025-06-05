import logging
from datetime import datetime, timedelta, timezone

import pytest
from django.conf import settings
from django.db import connection
from django.test import override_settings
from elasticsearch import Elasticsearch
from model_bakery import baker

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.broker.lookups import EXTERNAL_DATA_TYPE_DICT
from usaspending_api.common.elasticsearch.elasticsearch_sql_helpers import ensure_view_exists
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.conftest_helpers import TestElasticSearchIndex
from usaspending_api.etl.elasticsearch_loader_helpers import (
    delete_awards,
    delete_transactions,
    set_final_index_config,
)
from usaspending_api.etl.elasticsearch_loader_helpers.controller import PostgresElasticsearchIndexerController
from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import (
    _check_awards_for_deletes,
    _lookup_deleted_award_keys,
    delete_docs_by_unique_key,
)
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import ES_AWARDS_UNIQUE_KEY_FIELD
from usaspending_api.etl.management.commands.elasticsearch_indexer import (
    Command as ElasticsearchIndexerCommand,
)
from usaspending_api.etl.management.commands.elasticsearch_indexer import (
    parse_cli_args,
)
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.search.models import AwardSearch, TransactionSearch
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables


@pytest.fixture
def award_data_fixture(db):
    external_load_dates = [
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["transaction_fabs"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["transaction_fpds"],
            "last_load_date": datetime(2021, 1, 30, 12, 0, 0, 0, timezone.utc),
        },
        {
            "external_data_type__external_data_type_id": EXTERNAL_DATA_TYPE_DICT["es_deletes"],
            "last_load_date": datetime(2021, 1, 17, 16, 0, 0, 0, timezone.utc),
        },
    ]
    for load_date in external_load_dates:
        baker.make("broker.ExternalDataLoadDate", **load_date)

    fpds_unique_key = "fpds_transaction_id_1".upper()  # our ETL UPPERs all these when brought from Broker
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        award_category="contracts",
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_unique_id=fpds_unique_key,
        detached_award_proc_unique=fpds_unique_key,
        recipient_location_zip5="abcde",
        piid="IND12PB00323",
        federal_action_obligation=0,
        recipient_location_county_code="059",
        recipient_location_state_code="VA",
        recipient_location_congressional_code="11",
        recipient_location_country_code="USA",
        pop_state_code="VA",
        pop_congressional_code="11",
        pop_country_code="USA",
        naics_code="331122",
        product_or_service_code="1510",
        type_set_aside="8AN",
        type_of_contract_pricing="2",
        extent_competed="F",
        generated_unique_award_id="CONT_AWD_IND12PB00323",
    )

    fabs_unique_key = "fabs_transaction_id_2".upper()  # our ETL UPPERs all these when brought from Broker
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award_id=2,
        action_date="2016-10-01",
        is_fpds=False,
        type="02",
        award_category="grants",
        transaction_unique_id=fabs_unique_key,
        fain="P063P100612",
        cfda_number="84.063",
        afa_generated_unique=fabs_unique_key,
        generated_unique_award_id="ASST_NON_P063P100612",
    )

    baker.make(
        "references.ToptierAgency", toptier_agency_id=1, name="Department of Transportation", _fill_optional=True
    )
    baker.make(
        "references.SubtierAgency", subtier_agency_id=1, name="Department of Transportation", _fill_optional=True
    )
    baker.make("references.Agency", id=1, toptier_agency_id=1, subtier_agency_id=1, _fill_optional=True)
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="CONT_AWD_IND12PB00323",
        latest_transaction_id=1,
        earliest_transaction_search_id=1,
        latest_transaction_search_id=1,
        is_fpds=True,
        type="A",
        category="contracts",
        piid="IND12PB00323",
        description="pop tarts and assorted cereals",
        total_obligation=500000.00,
        date_signed="2010-10-1",
        awarding_agency_id=1,
        funding_agency_id=1,
        update_date="2012-05-19",
        action_date="2012-05-19",
        subaward_count=0,
        transaction_unique_id=fpds_unique_key,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="ASST_NON_P063P100612",
        latest_transaction_id=2,
        earliest_transaction_search_id=2,
        latest_transaction_search_id=2,
        is_fpds=False,
        type="02",
        category="grants",
        fain="P063P100612",
        total_obligation=1000000.00,
        date_signed="2016-10-01",
        update_date="2014-07-21",
        action_date="2016-10-01",
        subaward_count=0,
        transaction_unique_id=fabs_unique_key,
    )
    baker.make(
        "search.AwardSearch",
        award_id=3,
        generated_unique_award_id="'CONT_AWD_IND12PB00323",
        latest_transaction_id=1,
        earliest_transaction_search_id=1,
        latest_transaction_search_id=1,
        is_fpds=True,
        type="A",
        category="contracts",
        piid="IND12PB00323",
        description="pop tarts and assorted cereals",
        total_obligation=500000.00,
        date_signed="2010-10-1",
        awarding_agency_id=1,
        funding_agency_id=1,
        update_date="2012-05-19",
        action_date="2012-05-19",
        subaward_count=0,
        transaction_unique_id=fpds_unique_key,
    )
    baker.make("accounts.FederalAccount", id=1)
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=1,
        agency_id="097",
        main_account_code="4930",
        federal_account_id=1,
    )

    baker.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=1, treasury_account_id=1)


@pytest.fixture
def recipient_data_fixture(db):
    baker.make(
        "recipient.RecipientProfile",
        id="01",
        recipient_level="C",
        recipient_hash="521bb024-054c-4c81-8615-372f81629664",
        uei="UEI-01",
        recipient_name="Recipient 1",
    )
    baker.make(
        "recipient.RecipientProfile",
        id="02",
        recipient_level="P",
        recipient_hash="a70b86c3-5a12-4623-963b-9d96c4810163",
        uei="UEI-02",
        recipient_name="Recipient 2",
    )
    baker.make(
        "recipient.RecipientProfile",
        id="03",
        recipient_level="R",
        recipient_hash="9159db20-d2f7-42d4-88e2-a69759987520",
        uei="UEI-03",
        recipient_name="Recipient 3",
    )


@pytest.fixture
def location_data_fixture(db):
    baker.make(
        "search.TransactionSearch",
        transaction_id=100,
        is_fpds=False,
        transaction_unique_id="TRANSACTION100",
        pop_country_name="UNITED STATES",
        pop_state_name="CALIFORNIA",
        pop_city_name="LOS ANGELES",
        pop_county_name="LOS ANGELES",
        pop_zip5=90001,
        pop_congressional_code_current="34",
        pop_congressional_code="34",
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="NEVADA",
        recipient_location_city_name="LAS VEGAS",
        recipient_location_county_name="CLARK",
        recipient_location_zip5=88901,
        recipient_location_congressional_code_current="01",
        recipient_location_congressional_code="01",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=101,
        is_fpds=False,
        transaction_unique_id="TRANSACTION101",
        pop_country_name="DENMARK",
        pop_state_name=None,
        pop_city_name=None,
        pop_county_name=None,
        pop_zip5=None,
        pop_congressional_code_current=None,
        pop_congressional_code=None,
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="TEXAS",
        recipient_location_city_name="DALLAS",
        recipient_location_county_name="DALLAS",
        recipient_location_zip5=75001,
        recipient_location_congressional_code_current="30",
        recipient_location_congressional_code="30",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=102,
        is_fpds=False,
        transaction_unique_id="TRANSACTION102",
        pop_country_name="DENMARK",
        pop_state_name=None,
        pop_city_name=None,
        pop_county_name=None,
        pop_zip5=None,
        pop_congressional_code_current=None,
        pop_congressional_code=None,
        recipient_location_country_name="UNITED STATES",
        recipient_location_state_name="FAKE STATE",
        recipient_location_city_name="FAKE CITY",
        recipient_location_county_name="FAKE COUNTY",
        recipient_location_zip5=75001,
        recipient_location_congressional_code_current="30",
        recipient_location_congressional_code="30",
    )
    baker.make("recipient.StateData", id=1, fips="06", code="CA", name="California", type="state", year=2024)
    baker.make("recipient.StateData", id=2, fips="32", code="NV", name="Nevada", type="state", year=2024)
    baker.make("recipient.StateData", id=3, fips="48", code="TX", name="Texas", type="state", year=2024)


def mock_execute_sql(sql, results, verbosity=None):
    """SQL method is being mocked here since the `execute_sql_statement` used
    doesn't use the same DB connection to avoid multiprocessing errors
    """
    return execute_sql_to_ordered_dictionary(sql)


def test_create_and_load_new_award_index(award_data_fixture, elasticsearch_award_index, monkeypatch, caplog):
    """Test the ``elasticsearch_loader`` django management command to create a new awards index and load it
    with data from the DB
    """
    client = elasticsearch_award_index.client  # type: Elasticsearch
    monkeypatch.setattr("usaspending_api.etl.elasticsearch_loader_helpers.index_config.logger", logging.getLogger())

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_award_index.index_name)
    original_db_awards_count = Award.objects.count()

    # Inject ETL arg into config for this run, which loads a newly created index
    elasticsearch_award_index.etl_config["create_new_index"] = True
    try:
        _process_es_etl_test_config(client, elasticsearch_award_index)
    except SystemExit:
        assert (
            f"The earliest Transaction / Award load date of 2021-01-30 12:00:00+00:00 is later than the value"
            f" of 'es_deletes' (2021-01-17 16:00:00+00:00). To reduce the amount of data loaded in the next incremental"
            " load the values of 'es_deletes', 'es_awards', and 'es_transactions' should most likely be updated to"
            f" 2021-01-30 12:00:00+00:00 before proceeding. This recommendation assumes that the 'rpt' tables are"
            f" up to date. Optionally, this can be bypassed with the '--skip-date-check' option."
        ) in caplog.records[-1].message
    else:
        assert False, "No exception or the wrong exception was raised"

    es_etl_config = _process_es_etl_test_config(client, elasticsearch_award_index, options={"skip_date_check": True})

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    loader = PostgresElasticsearchIndexerController(es_etl_config)
    assert loader.__class__.__name__ == "PostgresElasticsearchIndexerController"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    # Along with other things, this will refresh the index, to surface loaded docs
    set_final_index_config(client, elasticsearch_award_index.index_name)

    assert client.indices.exists(elasticsearch_award_index.index_name)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count


def test_create_and_load_new_recipient_index(recipient_data_fixture, elasticsearch_recipient_index, monkeypatch):
    """Test the `elasticsearch_indexer` Django management command to create and load a new recipients index."""

    client = elasticsearch_recipient_index.client

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_recipient_index.index_name)

    original_db_recipients_count = RecipientProfile.objects.count()

    setup_elasticsearch_test(monkeypatch, elasticsearch_recipient_index)
    assert client.indices.exists(elasticsearch_recipient_index.index_name)

    es_recipient_docs = client.count(index=elasticsearch_recipient_index.index_name)["count"]
    assert es_recipient_docs == original_db_recipients_count


@pytest.mark.django_db(transaction=True)
def test_create_and_load_new_location_index(location_data_fixture, elasticsearch_location_index, monkeypatch):
    """Test the `elasticsearch_indexer` Django management command to create and load a new locations index"""

    client: Elasticsearch = elasticsearch_location_index.client

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_location_index.index_name)

    setup_elasticsearch_test(monkeypatch, elasticsearch_location_index)
    assert client.indices.exists(elasticsearch_location_index.index_name)

    es_location_docs = client.count(index=elasticsearch_location_index.index_name)["count"]

    ensure_view_exists(settings.ES_LOCATIONS_ETL_VIEW_NAME)
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM location_delta_view")
        db_response = cursor.fetchone()

    assert es_location_docs == db_response[0]


def test_create_and_load_new_transaction_index(award_data_fixture, elasticsearch_transaction_index, monkeypatch):
    """Test the ``elasticsearch_loader`` django management command to create a new transactions index and load it
    with data from the DB
    """
    client = elasticsearch_transaction_index.client  # type: Elasticsearch

    # Ensure index is not yet created
    assert not client.indices.exists(elasticsearch_transaction_index.index_name)
    original_db_tx_count = TransactionNormalized.objects.count()

    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

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
    awd = AwardSearch.objects.first()  # type: Award
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
    loader = PostgresElasticsearchIndexerController(es_etl_config)
    assert loader.__class__.__name__ == "PostgresElasticsearchIndexerController"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    client.indices.refresh(elasticsearch_award_index.index_name)

    assert client.indices.exists(elasticsearch_award_index.index_name)
    es_award_docs = client.count(index=elasticsearch_award_index.index_name)["count"]
    assert es_award_docs == original_db_awards_count
    es_awards = client.search(index=elasticsearch_award_index.index_name)
    updated_award = [a for a in es_awards["hits"]["hits"] if a["_source"]["award_id"] == awd.award_id][0]
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
    tx = TransactionSearch.objects.first()  # type: TransactionSearch
    tx.federal_action_obligation = 9999
    tx.etl_update_date = datetime.now(timezone.utc)
    tx.save()

    # Must use mock sql function to share test DB conn+transaction in ETL code
    # Patching on the module into which it is imported, not the module where it is defined
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.extract_data.execute_sql_statement", mock_execute_sql
    )
    # Also override SQL function listed in config object with the mock one
    es_etl_config["execute_sql_func"] = mock_execute_sql
    ensure_view_exists(es_etl_config["sql_view"], force=True)
    loader = PostgresElasticsearchIndexerController(es_etl_config)
    assert loader.__class__.__name__ == "PostgresElasticsearchIndexerController"
    loader.prepare_for_etl()
    loader.dispatch_tasks()
    client.indices.refresh(elasticsearch_transaction_index.index_name)

    assert client.indices.exists(elasticsearch_transaction_index.index_name)
    es_tx_docs = client.count(index=elasticsearch_transaction_index.index_name)["count"]
    assert es_tx_docs == original_db_txs_count
    es_txs = client.search(index=elasticsearch_transaction_index.index_name)
    updated_tx = [t for t in es_txs["hits"]["hits"] if t["_source"]["transaction_id"] == tx.transaction_id][0]
    assert int(float(updated_tx["_source"]["federal_action_obligation"])) == 9999


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


@pytest.mark.django_db(transaction=True)
def test__check_awards_for_deletes(
    award_data_fixture, monkeypatch, spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):
    test_config = {"verbose": False}
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data.execute_sql_statement", mock_execute_sql
    )
    id_list = ["CONT_AWD_IND12PB00323"]
    awards = _check_awards_for_deletes(test_config, id_list)
    assert awards == []

    id_list = ["CONT_AWD_WHATEVER", "CONT_AWD_IND12PB00323"]
    awards = _check_awards_for_deletes(test_config, id_list)
    assert awards == ["CONT_AWD_WHATEVER"]

    # Check to ensure sql properly escapes characters
    id_list = ["CONT_AWD_SOMETHING_BEFORE", "'CONT_AWD_IND12PB00323", "CONT_AWD_SOMETHING_AFTER"]
    awards = _check_awards_for_deletes(test_config, id_list)
    assert sorted(awards) == [
        "CONT_AWD_SOMETHING_AFTER",
        "CONT_AWD_SOMETHING_BEFORE",
    ]

    tables_to_load = ["awards"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    id_list = ["CONT_AWD_SOMETHING_BEFORE", "'CONT_AWD_IND12PB00323", "CONT_AWD_SOMETHING_AFTER"]
    awards = _check_awards_for_deletes(test_config, id_list, spark)
    assert sorted(awards) == [
        "CONT_AWD_SOMETHING_AFTER",
        "CONT_AWD_SOMETHING_BEFORE",
    ]


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
    deleted_tx = [key for key in fpds_keys + fabs_keys]
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg, delete_window_start, fabs_external_data_load_data_key, fpds_external_data_load_data_key: deleted_tx,
    )

    original_db_awards_count = Award.objects.count()
    # Simulate an awards ETL deleting the transactions and awards from the DB.
    TransactionSearch.objects.all().delete()
    AwardSearch.objects.all().delete()

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
    assert es_award_docs == 1


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
        lambda cfg, delete_window_start, fabs_external_data_load_data_key, fpds_external_data_load_data_key: [
            "unmatchable_tx_key1",
            "unmatchable_tx_key2",
            "unmatchable_tx_key3",
        ],
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
    deleted_tx = ["ASST_TX_" + tx.transaction_unique_id.upper()]
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg, delete_window_start, fabs_external_data_load_data_key, fpds_external_data_load_data_key: deleted_tx,
    )

    original_db_awards_count = Award.objects.count()
    # Simulate an awards ETL deleting the transactions and awards from the DB.
    TransactionSearch.objects.filter(transaction_id=tx.id).delete()
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
    deleted_tx = ["ASST_TX_" + tx.transaction_unique_id.upper()]
    # Patch the function that fetches deleted transaction keys from the CSV delete-log file
    # in S3, and provide fake transaction keys
    monkeypatch.setattr(
        "usaspending_api.etl.elasticsearch_loader_helpers.delete_data._gather_deleted_transaction_keys",
        lambda cfg, delete_window_start, fabs_external_data_load_data_key, fpds_external_data_load_data_key: deleted_tx,
    )

    original_db_tx_count = TransactionNormalized.objects.count()
    # Simulate an awards ETL deleting the transaction and award from the DB.
    TransactionSearch.objects.filter(transaction_id=tx.id).delete()
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


def _process_es_etl_test_config(
    client: Elasticsearch, test_es_index: TestElasticSearchIndex, options: dict | None = None
):
    """Use the Django mgmt cmd to extract args with default values, then update those with test ETL config values"""
    cmd = ElasticsearchIndexerCommand()
    cmd_name = cmd.__module__.split(".")[-1]  # should give "elasticsearch_indexer" unless name changed
    parser = cmd.create_parser("", cmd_name)
    # Changes dict of arg k-v pairs into a flat list of ordered ["k1", "v1", "k2", "v2" ...] items
    list_of_arg_kvs = [["--" + k.replace("_", "-"), str(v)] for k, v in test_es_index.etl_config.items()]
    test_args = [arg_item for kvpair in list_of_arg_kvs for arg_item in kvpair]
    cli_args, _ = parser.parse_known_args(args=test_args)  # parse the known args programmatically
    cli_opts = {**vars(cli_args), **test_es_index.etl_config}  # update defaults with test config
    if options:
        cli_opts.update(options)
    es_etl_config = parse_cli_args(cli_opts, client)  # use command's config parser for final config for testing ETL
    es_etl_config["slices"] = "auto"  # no need to calculate slices for testing
    return es_etl_config
