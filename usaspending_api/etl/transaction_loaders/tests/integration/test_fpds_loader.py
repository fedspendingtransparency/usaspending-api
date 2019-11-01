import logging
import random
import datetime
import pytest

from django.db import connections
from django.test import TestCase

import usaspending_api.etl.transaction_loaders.fpds_loader as fpds_loader
from usaspending_api.awards.models import TransactionFPDS
from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
    legal_entity_nonboolean_columns,
    legal_entity_boolean_columns,
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
        load_object = [dict(detached_award_procurement=_) for _ in broker_objects]
        connection = connections["data_broker"]
        with connection.cursor() as cursor:
            columns, values = format_bulk_insert_list_column_sql(cursor, load_object, "detached_award_procurement")
            insert_sql = (
                "INSERT INTO detached_award_procurement {} VALUES {} RETURNING detached_award_procurement_id"
            ).format(columns, values)
            cursor.execute(insert_sql)
            created_transactions = cursor.fetchall()
            self.logger.warning("created_transactions = " + str(created_transactions))

        # TODO:Remove - just used during debug to show there _is_ data in the broker db
        # with connection.cursor() as cursor:
        #     cursor.execute("select * from detached_award_procurement")
        #     created_transactions = cursor.fetchall()
        #     self.logger.warning("created_transactions = " + str(created_transactions))

        fpds_loader.load_ids(broker_id_list)
        # usaspending_transactions = TransactionFPDS.objects.all()
        # assert len(usaspending_transactions) == 3
        # assert 101 in [_.transaction.id for _ in usaspending_transactions]
        # assert 201 in [_.transaction.id for _ in usaspending_transactions]
        # assert 301 in [_.transaction.id for _ in usaspending_transactions]

        for obj in TransactionFPDS.objects.all():
            self.logger.info("Found FPDS Transaction with id = {}".format(obj.transaction.id))

        # TODO:Make asserts on the data
        # Compare data is as expected
        # assert load_objects_pre_transaction["award"]["transaction_unique_id"] == str(dummy_broker_ids[0])
        # assert load_objects_pre_transaction["transaction_normalized"]["transaction_unique_id"] == str(dummy_broker_ids[0])
        # assert load_objects_pre_transaction["transaction_normalized"]["award_id"] == final_award_id
        # assert load_objects_pre_transaction["transaction_normalized"]["funding_agency_id"] == 1
        # assert load_objects_pre_transaction["transaction_normalized"]["awarding_agency_id"] == 1
        # assert 2001 <= load_objects_pre_transaction["transaction_normalized"]["fiscal_year"] <= 2019


def _assemble_dummy_broker_data():
    return {
        **transaction_fpds_nonboolean_columns,
        **transaction_normalized_nonboolean_columns,
        **legal_entity_nonboolean_columns,
        **{key: str(random.choice([True, False])) for key in legal_entity_boolean_columns},
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

        # Proper values for data types
        dummy_row["federal_action_obligation"] = 1000001
        results.append(dummy_row)
    return results
