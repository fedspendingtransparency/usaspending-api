import datetime
import pytest

from django.core.management import call_command
from model_bakery import baker

from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate, ExternalDataType
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.search.models import TransactionSearch, AwardSearch
from usaspending_api.transactions.models import SourceAssistanceTransaction


def _assemble_source_assistance_records(id_list):
    """Return a list of dictionaries, in the same structure as `fetchall` when using psycopg2.extras.DictCursor"""
    for trx_id in id_list:
        dummy_row = {
            "published_fabs_id": trx_id,
            "afa_generated_unique": f"{trx_id}",
            "submission_id": trx_id,
            "fain": "FAIN",
            "uri": "URI",
            "unique_award_key": f"UNIQUE_AWARD_KEY",
            "action_date": "2010-01-01 00:00:00",
            "created_at": datetime.datetime.fromtimestamp(0),
            "updated_at": datetime.datetime.fromtimestamp(0),
            "modified_at": datetime.datetime.utcnow(),
            "federal_action_obligation": 1000000 + trx_id,
            "face_value_loan_guarantee": 2000000 + trx_id,
            "indirect_federal_sharing": 3000000 + trx_id,
            "original_loan_subsidy_cost": 4000000 + trx_id,
            "non_federal_funding_amount": 5000000 + trx_id,
            "total_funding_amount": f"{6000000 + trx_id}",
            "period_of_performance_star": "2010-01-01 00:00:00",
            "high_comp_officer1_amount": 1 * 1000000,
            "high_comp_officer2_amount": 2 * 1000000,
            "high_comp_officer3_amount": 3 * 1000000,
            "high_comp_officer4_amount": 4 * 1000000,
            "high_comp_officer5_amount": 5 * 1000000,
            "is_active": True,
        }

        dummy_row["period_of_performance_curr"] = datetime.datetime.strptime(
            dummy_row["period_of_performance_star"], "%Y-%m-%d %H:%M:%S"
        ) + datetime.timedelta(days=365)

        dummy_row["action_date"] = datetime.datetime.strptime(
            dummy_row["action_date"], "%Y-%m-%d %H:%M:%S"
        ) + datetime.timedelta(days=dummy_row["published_fabs_id"])

        baker.make("transactions.SourceAssistanceTransaction", **dummy_row, _fill_optional=True)


@pytest.mark.django_db
@pytest.mark.skip(reason="Test based on pre-databricks loader code. Remove when fully cut over.")
def test_load_source_assistance_by_ids():
    """
    Simple end-to-end integration test to exercise the FABS loader given 3 records in an actual broker database
    to load into an actual usaspending database
    """
    source_assistance_id_list = [101, 201, 301]
    _assemble_source_assistance_records(source_assistance_id_list)

    # Run core logic to be tested
    call_command("fabs_nightly_loader", "--ids", *source_assistance_id_list)

    # Lineage should trace back to the broker records
    usaspending_transactions = TransactionFABS.objects.all()
    assert len(usaspending_transactions) == 3
    tx_fabs_broker_ref_ids = [_.published_fabs_id for _ in usaspending_transactions]
    assert 101 in tx_fabs_broker_ref_ids
    assert 201 in tx_fabs_broker_ref_ids
    assert 301 in tx_fabs_broker_ref_ids
    tx_fabs_broker_ref_id_strings = [_.afa_generated_unique for _ in usaspending_transactions]
    assert "101" in tx_fabs_broker_ref_id_strings
    assert "201" in tx_fabs_broker_ref_id_strings
    assert "301" in tx_fabs_broker_ref_id_strings
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
    assert new_award.transaction_unique_id == "NONE"

    # Award's latest_transaction should point to transaction with the latest action_date
    assert new_award.latest_transaction.transaction_unique_id == "301"

    # Award's earliest_transaction should point to transaction with the earliest action_date
    assert new_award.earliest_transaction.transaction_unique_id == "101"

    # Given action_date additions, the 3rd one should push into 2011
    transactions_by_id = {
        transaction.published_fabs_id: transaction.transaction for transaction in usaspending_transactions
    }
    assert transactions_by_id[101].fiscal_year == 2010
    assert transactions_by_id[201].fiscal_year == 2010
    assert transactions_by_id[301].fiscal_year == 2011

    # Verify that amounts are populated and summed correctly
    expected_award_amounts = {
        "non_federal_funding_amount": [15000603],
        "total_funding_amount": [18000603],
        "total_loan_value": [6000603],
        "total_indirect_federal_sharing": [9000603],
        "total_obligation": [3000603],
        "total_subsidy_cost": [12000603],
    }
    award_values = list(Award.objects.values(*expected_award_amounts))
    for field, expected_values in expected_award_amounts.items():
        assert sorted([val[field] for val in award_values]) == expected_values, f"incorrect value for field: {field}"

    expected_transaction_normalized_amounts = {
        "non_federal_funding_amount": [5000101, 5000201, 5000301],
        "funding_amount": [6000101, 6000201, 6000301],
        "face_value_loan_guarantee": [2000101, 2000201, 2000301],
        "indirect_federal_sharing": [3000101, 3000201, 3000301],
        "federal_action_obligation": [1000101, 1000201, 1000301],
        "original_loan_subsidy_cost": [4000101, 4000201, 4000301],
    }
    transaction_normalized_values = list(TransactionNormalized.objects.values(*expected_transaction_normalized_amounts))
    for field, expected_values in expected_transaction_normalized_amounts.items():
        assert (
            sorted([val[field] for val in transaction_normalized_values]) == expected_values
        ), f"incorrect value for field: {field}"

    expected_transaction_fabs_amounts = {
        "non_federal_funding_amount": [5000101, 5000201, 5000301],
        "total_funding_amount": ["6000101", "6000201", "6000301"],
        "face_value_loan_guarantee": [2000101, 2000201, 2000301],
        "indirect_federal_sharing": [3000101, 3000201, 3000301],
        "federal_action_obligation": [1000101, 1000201, 1000301],
        "original_loan_subsidy_cost": [4000101, 4000201, 4000301],
    }
    transaction_fabs_values = list(TransactionFABS.objects.values(*expected_transaction_fabs_amounts))
    for field, expected_values in expected_transaction_fabs_amounts.items():
        assert (
            sorted([val[field] for val in transaction_fabs_values]) == expected_values
        ), f"incorrect value for field: {field}"


@pytest.mark.django_db(transaction=True)
@pytest.mark.skip(reason="Test based on pre-databricks loader code. Remove when fully cut over.")
def test_delete_fabs_success(monkeypatch):
    # Award/Transaction deleted based on 1-1 transaction
    baker.make(AwardSearch, award_id=1, generated_unique_award_id="TEST_AWARD_1")
    baker.make(
        TransactionSearch,
        transaction_id=1,
        award_id=1,
        generated_unique_award_id="TEST_AWARD_1",
        published_fabs_id=301,
    )

    # Award kept despite having one of their associated transactions removed
    baker.make(AwardSearch, award_id=2, generated_unique_award_id="TEST_AWARD_2")
    baker.make(
        TransactionSearch, transaction_id=2, award_id=2, generated_unique_award_id="2019-01-01", published_fabs_id=302
    )
    baker.make(
        TransactionSearch, transaction_id=3, award_id=2, generated_unique_award_id="2019-01-02", published_fabs_id=303
    )

    # Award/Transaction untouched at all as control
    baker.make(AwardSearch, award_id=3, generated_unique_award_id="TEST_AWARD_3")
    baker.make(
        TransactionSearch,
        transaction_id=4,
        award_id=3,
        generated_unique_award_id="TEST_AWARD_3",
        published_fabs_id=304,
    )

    # Award is not deleted; old transaction deleted; new transaction uses old award
    baker.make(AwardSearch, award_id=4, generated_unique_award_id="TEST_AWARD_4")
    baker.make(
        TransactionSearch,
        transaction_id=5,
        award_id=4,
        generated_unique_award_id="TEST_AWARD_4",
        published_fabs_id=305,
    )
    baker.make(
        SourceAssistanceTransaction,
        published_fabs_id=306,
        afa_generated_unique="TEST_TRANSACTION_6",
        unique_award_key="TEST_AWARD_4",
        created_at="2022-02-18 18:27:50",
        updated_at="2022-02-18 18:27:50",
        action_date="2022-02-18 18:27:50",
        modified_at=datetime.datetime.utcnow(),
        is_active=True,
    )
    baker.make(ExternalDataType, external_data_type_id=2, name="fabs")
    baker.make(ExternalDataLoadDate, external_data_type_id=2, last_load_date="2022-02-01 18:27:50")

    # Make sure current Awards and Transactions are linked
    update_awards()

    # Make sure we get the correct source transaction to delete
    monkeypatch.setattr(
        "usaspending_api.broker.management.commands.fabs_nightly_loader.retrieve_deleted_fabs_transactions",
        lambda start_datetime, end_datetime=None: {
            "2022-02-18": ["TEST_TRANSACTION_1", "TEST_TRANSACTION_2", "TEST_TRANSACTION_5"]
        },
    )
    monkeypatch.setattr(
        "usaspending_api.broker.management.commands.fabs_nightly_loader.get_delete_pks_for_afa_keys",
        lambda afa_ids_to_delete: [301, 302, 305],
    )

    # Main call
    call_command("fabs_nightly_loader")

    # Awards
    awards_left = Award.objects.all()
    award_ids_left = set([award.id for award in awards_left])
    expected_awards_ids_left = [2, 3, 4]
    assert sorted(award_ids_left) == expected_awards_ids_left

    latest_transaction_ids = set([award.latest_transaction_id for award in awards_left])
    new_award_transaction_id = TransactionNormalized.objects.filter(award_id=4).values_list("id", flat=True).first()
    expected_latest_transaction_ids = sorted([3, 4, new_award_transaction_id])
    assert sorted(latest_transaction_ids) == expected_latest_transaction_ids

    # Transaction Normalized
    transactions_left = TransactionNormalized.objects.all()

    transaction_norm_ids_left = set([transaction.id for transaction in transactions_left])
    expected_transaction_norm_ids_left = sorted([3, 4, new_award_transaction_id])
    assert sorted(transaction_norm_ids_left) == expected_transaction_norm_ids_left

    # Transaction FABS
    transaction_fabs_left = TransactionFABS.objects.all()

    transaction_fabs_ids_left = set([transaction_fabs.published_fabs_id for transaction_fabs in transaction_fabs_left])
    expected_transaction_fabs_left = [303, 304, 306]
    assert sorted(transaction_fabs_ids_left) == expected_transaction_fabs_left
