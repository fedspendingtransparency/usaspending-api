import pytest

from collections import OrderedDict
from datetime import datetime, timezone
from model_mommy import mommy
from pathlib import Path
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.helpers.text_helpers import generate_random_string
from usaspending_api.etl.es_etl_helpers import configure_sql_strings, check_awards_for_deletes, get_deleted_award_ids
from usaspending_api.etl.rapidloader import Rapidloader


@pytest.fixture
def award_data_fixture(db):
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_unique_id="transaction_id_1",
    )
    mommy.make(
        "awards.TransactionFPDS",
        detached_award_proc_unique="transaction_id_1",
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

    mommy.make("awards.TransactionNormalized", id=2, award_id=2, action_date="2016-10-01", is_fpds=False, type="02")
    mommy.make("awards.TransactionFABS", transaction_id=2, fain="P063P100612", cfda_number="84.063")

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


@pytest.fixture
def baby_sleeps(monkeypatch):
    """
    If we don't do this, sleeps add about 2 minutes to the tests in this file.  Setting sleeps to
    None or 0 was causing exceptions to be thrown, thus the 0.001 value.
    """
    from time import sleep

    def _sleep(seconds):
        sleep(0.001)

    monkeypatch.setattr("usaspending_api.etl.es_etl_helpers.sleep", _sleep)
    monkeypatch.setattr("usaspending_api.etl.rapidloader.sleep", _sleep)


config = {
    "root_index": "award-query",
    "processing_start_datetime": datetime(2019, 12, 13, 16, 10, 33, 729108, tzinfo=timezone.utc),
    "verbose": False,
    "load_type": "awards",
    "process_deletes": False,
    "directory": Path(__file__).resolve().parent,
    "skip_counts": False,
    "index_name": "test-{}-{}".format(
        datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f"), generate_random_string()
    ),
    "create_new_index": True,
    "snapshot": False,
    "fiscal_years": [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020],
    "starting_date": datetime(2007, 10, 1, 0, 0, tzinfo=timezone.utc),
    "max_query_size": 10000,
    "is_incremental_load": False,
}


def test_es_award_loader_class(award_data_fixture, elasticsearch_award_index, baby_sleeps):
    elasticsearch_client = instantiate_elasticsearch_client()
    loader = Rapidloader(config, elasticsearch_client)
    assert loader.__class__.__name__ == "Rapidloader"
    loader.run_load_steps()
    assert elasticsearch_client.indices.exists(config["index_name"])
    elasticsearch_client.indices.delete(index=config["index_name"], ignore_unavailable=False)


def test_es_transaction_loader_class(award_data_fixture, elasticsearch_transaction_index, baby_sleeps):
    config["root_index"] = "transaction-query"
    config["load_type"] = "transactions"
    elasticsearch_client = instantiate_elasticsearch_client()
    loader = Rapidloader(config, elasticsearch_client)
    assert loader.__class__.__name__ == "Rapidloader"
    loader.run_load_steps()
    assert elasticsearch_client.indices.exists(config["index_name"])
    elasticsearch_client.indices.delete(index=config["index_name"], ignore_unavailable=False)


def test_configure_sql_strings():
    config["fiscal_year"] = 2019
    config["root_index"] = "award-query"
    config["load_type"] = "awards"
    copy, id, count = configure_sql_strings(config, "filename", [1])
    copy_sql = """"COPY (
    SELECT *
    FROM award_delta_view
    WHERE fiscal_year=2019 AND update_date >= '2007-10-01'
) TO STDOUT DELIMITER ',' CSV HEADER" > 'filename'
"""
    count_sql = """
SELECT COUNT(*) AS count
FROM award_delta_view
WHERE fiscal_year=2019 AND update_date >= '2007-10-01'
"""
    assert copy == copy_sql
    assert count == count_sql


# SQL method is being mocked here since the `execute_sql_statement` used doesn't use the same DB connection to avoid multiprocessing errors
def mock_execute_sql(sql, results):
    return execute_sql_to_ordered_dictionary(sql)


def test_award_delete_sql(award_data_fixture, monkeypatch, db):
    monkeypatch.setattr("usaspending_api.etl.es_etl_helpers.execute_sql_statement", mock_execute_sql)
    id_list = ["CONT_AWD_IND12PB00323"]
    awards = check_awards_for_deletes(id_list)
    assert awards == []

    id_list = ["CONT_AWD_WHATEVER", "CONT_AWD_IND12PB00323"]
    awards = check_awards_for_deletes(id_list)
    assert awards == [OrderedDict([("generated_unique_award_id", "CONT_AWD_WHATEVER")])]


def test_get_award_ids(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()
    id_list = [{"key": 1, "col": "transaction_id"}]
    config["root_index"] = "transaction-query"
    config["load_type"] = "transactions"
    client = elasticsearch_transaction_index.client
    ids = get_deleted_award_ids(client, id_list, config, index=elasticsearch_transaction_index.index_name)
    assert ids == ["CONT_AWD_IND12PB00323"]
