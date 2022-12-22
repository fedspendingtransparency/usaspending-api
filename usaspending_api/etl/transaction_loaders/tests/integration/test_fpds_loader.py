import random
import datetime
import pytest

from django.core.management import call_command
from model_bakery import baker

from usaspending_api.awards.models import Award, TransactionFPDS, TransactionNormalized
from usaspending_api.broker.models import ExternalDataLoadDate, ExternalDataType
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_boolean_columns,
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
)
from usaspending_api.search.models import AwardSearch
from usaspending_api.transactions.models import SourceProcurementTransaction


def _assemble_dummy_source_data():
    """
    Simple way to reuse the field mappings as a def of all required fields, and then use that as the basis for
    populating dummy data.
    """
    return {
        **transaction_fpds_nonboolean_columns,
        **transaction_normalized_nonboolean_columns,
        **{key: str(random.choice([True, False])) for key in transaction_fpds_boolean_columns},
    }


def _assemble_source_procurement_records(id_list):
    """Return a list of dictionaries, in the same structure as `fetchall` when using psycopg2.extras.DictCursor"""
    for trx_id in id_list:
        dummy_row = _assemble_dummy_source_data()
        dummy_row["detached_award_procurement_id"] = trx_id
        dummy_row["detached_award_proc_unique"] = str(trx_id)
        dummy_row["ordering_period_end_date"] = "2010-01-01 00:00:00"
        dummy_row["action_date"] = "2010-01-01 00:00:00"
        dummy_row["initial_report_date"] = "2010-01-01 00:00:00"
        dummy_row["solicitation_date"] = "2010-01-01 00:00:00"
        dummy_row["created_at"] = datetime.datetime.fromtimestamp(0)
        dummy_row["updated_at"] = datetime.datetime.fromtimestamp(0)
        dummy_row["federal_action_obligation"] = 1000001
        dummy_row["period_of_performance_star"] = "2010-01-01 00:00:00"
        dummy_row["period_of_performance_curr"] = datetime.datetime.strptime(
            dummy_row["period_of_performance_star"], "%Y-%m-%d %H:%M:%S"
        ) + datetime.timedelta(days=365)
        dummy_row["last_modified"] = datetime.datetime.utcnow()
        dummy_row["high_comp_officer1_amount"] = 1 * 1000000
        dummy_row["high_comp_officer2_amount"] = 2 * 1000000
        dummy_row["high_comp_officer3_amount"] = 3 * 1000000
        dummy_row["high_comp_officer4_amount"] = 4 * 1000000
        dummy_row["high_comp_officer5_amount"] = 5 * 1000000
        dummy_row["base_exercised_options_val"] = 10203
        dummy_row["base_and_all_options_value"] = 30201

        dummy_row["action_date"] = datetime.datetime.strptime(
            dummy_row["action_date"], "%Y-%m-%d %H:%M:%S"
        ) + datetime.timedelta(days=dummy_row["detached_award_procurement_id"])

        baker.make("transactions.SourceProcurementTransaction", **dummy_row)


@pytest.mark.django_db
@pytest.mark.skip(reason="Test based on pre-databricks loader code. Remove when fully cut over.")
def test_load_source_procurement_by_ids():
    """
    Simple end-to-end integration test to exercise the fpds loader given 3 records in an actual broker database
    to load into an actual usaspending database
    """
    source_procurement_id_list = [101, 201, 301]
    _assemble_source_procurement_records(source_procurement_id_list)

    # Run core logic to be tested
    call_command("load_fpds_transactions", "--ids", *source_procurement_id_list)

    # Lineage should trace back to the broker records
    usaspending_transactions = TransactionFPDS.objects.all()
    assert len(usaspending_transactions) == 3
    tx_fpds_broker_ref_ids = [_.detached_award_procurement_id for _ in usaspending_transactions]
    assert 101 in tx_fpds_broker_ref_ids
    assert 201 in tx_fpds_broker_ref_ids
    assert 301 in tx_fpds_broker_ref_ids
    tx_fpds_broker_ref_id_strings = [_.detached_award_proc_unique for _ in usaspending_transactions]
    assert "101" in tx_fpds_broker_ref_id_strings
    assert "201" in tx_fpds_broker_ref_id_strings
    assert "301" in tx_fpds_broker_ref_id_strings
    tx_norm_broker_ref_id_strings = [_.transaction.transaction_unique_id for _ in usaspending_transactions]
    assert "101" in tx_norm_broker_ref_id_strings
    assert "201" in tx_norm_broker_ref_id_strings
    assert "301" in tx_norm_broker_ref_id_strings

    # All 3 should be under the same award
    # 1 award created
    usaspending_awards = Award.objects.all()
    assert len(usaspending_awards) == 1
    new_award = usaspending_awards[0]

    # All transactions reference that award and its id
    tx_norm_awd = [_.transaction.award for _ in usaspending_transactions]
    assert usaspending_awards[0] == tx_norm_awd[0] == tx_norm_awd[1] == tx_norm_awd[2]
    tx_norm_awd_ids = [_.transaction.award_id for _ in usaspending_transactions]
    assert new_award.id == tx_norm_awd_ids[0] == tx_norm_awd_ids[1] == tx_norm_awd_ids[2]

    # And the single award created should refer to the first transaction processed that spawned the award (101)s
    assert new_award.transaction_unique_id == "101"

    # Award's latest_transaction should point to transaction with latest action_date
    assert new_award.latest_transaction.transaction_unique_id == "301"

    # Award's earliest_transaction should point to transaction with earliest action_date
    assert new_award.earliest_transaction.transaction_unique_id == "101"

    # Given action_date additions, the 3rd one should push into 2011
    transactions_by_id = {
        transaction.detached_award_procurement_id: transaction.transaction for transaction in usaspending_transactions
    }
    assert transactions_by_id[101].fiscal_year == 2010
    assert transactions_by_id[201].fiscal_year == 2010
    assert transactions_by_id[301].fiscal_year == 2011


@pytest.mark.django_db(transaction=True)
@pytest.mark.skip(reason="Test based on pre-databricks loader code. Remove when fully cut over.")
def test_delete_fpds_success(monkeypatch):
    # Award/Transaction deleted based on 1-1 transaction
    baker.make(AwardSearch, award_id=1, generated_unique_award_id="TEST_AWARD_1")
    baker.make(TransactionNormalized, id=1, award_id=1, unique_award_key="TEST_AWARD_1")
    baker.make(TransactionFPDS, transaction_id=1, detached_award_procurement_id=301, unique_award_key="TEST_AWARD_1")

    # Award kept despite having one of their associated transactions removed
    baker.make(AwardSearch, award_id=2, generated_unique_award_id="TEST_AWARD_2")
    baker.make(TransactionNormalized, id=2, award_id=2, action_date="2019-01-01", unique_award_key="TEST_AWARD_2")
    baker.make(TransactionNormalized, id=3, award_id=2, action_date="2019-01-02", unique_award_key="TEST_AWARD_2")
    baker.make(TransactionFPDS, transaction_id=2, detached_award_procurement_id=302, unique_award_key="TEST_AWARD_2")
    baker.make(TransactionFPDS, transaction_id=3, detached_award_procurement_id=303, unique_award_key="TEST_AWARD_2")

    # Award/Transaction untouched at all as control
    baker.make(AwardSearch, award_id=3, generated_unique_award_id="TEST_AWARD_3")
    baker.make(TransactionNormalized, id=4, award_id=3, unique_award_key="TEST_AWARD_3")
    baker.make(TransactionFPDS, transaction_id=4, detached_award_procurement_id=304, unique_award_key="TEST_AWARD_3")

    # Award is not deleted; old transaction deleted; new transaction uses old award
    baker.make(AwardSearch, award_id=4, generated_unique_award_id="TEST_AWARD_4")
    baker.make(TransactionNormalized, id=5, award_id=4, unique_award_key="TEST_AWARD_4")
    baker.make(TransactionFPDS, transaction_id=5, detached_award_procurement_id=305, unique_award_key="TEST_AWARD_4")
    baker.make(
        SourceProcurementTransaction,
        detached_award_procurement_id=306,
        detached_award_proc_unique="TEST_TRANSACTION_6",
        unique_award_key="TEST_AWARD_4",
        created_at="2022-02-18 18:27:50",
        updated_at="2022-02-18 18:27:50",
        action_date="2022-02-18 18:27:50",
    )
    baker.make(ExternalDataType, external_data_type_id=1, name="fpds")
    baker.make(ExternalDataLoadDate, external_data_type_id=1, last_load_date="2022-02-01 18:27:50")

    # Make sure current Awards and Transactions are linked
    update_awards()

    # Make sure we get the correct source transaction to delete
    monkeypatch.setattr(
        "usaspending_api.broker.management.commands.load_fpds_transactions.retrieve_deleted_fpds_transactions",
        lambda start_datetime, end_datetime=None: {"2022-02-18": [301, 302, 305]},
    )

    # Main call
    call_command("load_fpds_transactions", "--since-last-load")

    # Awards
    awards_left = Award.objects.all()
    award_ids_left = set([award.id for award in awards_left])
    expected_awards_ids_left = [2, 3, 4]
    assert sorted(award_ids_left) == expected_awards_ids_left
    assert len(award_ids_left) == len(expected_awards_ids_left)

    latest_transaction_ids = set([award.latest_transaction_id for award in awards_left])
    new_award_transaction_id = TransactionNormalized.objects.filter(award_id=4).values_list("id", flat=True).first()
    expected_latest_transaction_ids = sorted([3, 4, new_award_transaction_id])
    assert sorted(latest_transaction_ids) == expected_latest_transaction_ids

    # Transaction Normalized
    transactions_left = TransactionNormalized.objects.all()

    transaction_norm_ids_left = set([transaction.id for transaction in transactions_left])
    expected_transaction_norm_ids_left = sorted([3, 4, new_award_transaction_id])
    assert sorted(transaction_norm_ids_left) == expected_transaction_norm_ids_left

    # Transaction FPDS
    transactions_fpds_left = TransactionFPDS.objects.all()

    transaction_fpds_left = set(
        [transaction_fpds.detached_award_procurement_id for transaction_fpds in transactions_fpds_left]
    )
    expected_transaction_fpds_left = [303, 304, 306]
    assert sorted(transaction_fpds_left) == expected_transaction_fpds_left
