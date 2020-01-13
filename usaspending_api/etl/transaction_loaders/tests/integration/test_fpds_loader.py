import logging
import random
import datetime
import pytest

from django.core.management import call_command
from django.db import connections
from django.test import TestCase

from usaspending_api.awards.models import Award, TransactionFPDS
from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
    recipient_location_nonboolean_columns,
    place_of_performance_nonboolean_columns,
    transaction_fpds_boolean_columns,
)
from usaspending_api.etl.transaction_loaders.data_load_helpers import format_bulk_insert_list_column_sql


class FPDSLoaderIntegrationTestCase(TestCase):
    logger = logging.getLogger(__name__)
    multi_db = True

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @pytest.mark.usefixtures("broker_db_setup", "db")
    def test_load_ids_from_broker(self):
        """
        Simple end-to-end integration test to exercise the fpds loader given 3 records in an actual broker database
        to load into an actual usaspending database
        """
        broker_id_list = [101, 201, 301]
        broker_objects = _assemble_detached_award_procurement_records(broker_id_list)

        # Reset action_date for test purposes. Add the id as days to given action_date
        for broker_object in broker_objects:
            broker_object["action_date"] = datetime.datetime.strptime(
                broker_object["action_date"], "%Y-%m-%d %H:%M:%S"
            ) + datetime.timedelta(days=broker_object["detached_award_procurement_id"])

        load_object = [dict(detached_award_procurement=_) for _ in broker_objects]
        connection = connections["data_broker"]
        with connection.cursor() as cursor:
            columns, values = format_bulk_insert_list_column_sql(cursor, load_object, "detached_award_procurement")
            insert_sql = (
                "INSERT INTO detached_award_procurement {} VALUES {} RETURNING detached_award_procurement_id"
            ).format(columns, values)
            cursor.execute(insert_sql)

        # Run core logic to be tested
        call_command("load_fpds_transactions", "--ids", *broker_id_list)

        # Assert results

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
            transaction.detached_award_procurement_id: transaction for transaction in usaspending_transactions
        }
        assert transactions_by_id[101].fiscal_year == 2010
        assert transactions_by_id[201].fiscal_year == 2010
        assert transactions_by_id[301].fiscal_year == 2011


def _assemble_dummy_broker_data():
    """
    Simple way to reuse the field mappings as a def of all required fields, and then use that as the basis for
    populating dummy data.
    """
    return {
        **transaction_fpds_nonboolean_columns,
        **transaction_normalized_nonboolean_columns,
        **recipient_location_nonboolean_columns,
        **place_of_performance_nonboolean_columns,
        **{key: str(random.choice([True, False])) for key in transaction_fpds_boolean_columns},
    }


def _assemble_detached_award_procurement_records(id_list):
    """Return a list of dictionaries, in the same structure as `fetchall` when using psycopg2.extras.DictCursor"""
    results = []
    for id in id_list:
        dummy_row = _assemble_dummy_broker_data()
        dummy_row["detached_award_procurement_id"] = id
        dummy_row["detached_award_proc_unique"] = str(id)
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

        results.append(dummy_row)
    return results
