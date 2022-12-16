"""Automated Unit Tests for the the loading of transaction and award tables in Delta Lake.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence, Set

import pyspark
from pyspark.sql import SparkSession

from model_bakery import baker
from pytest import fixture, mark, raises

from django.db import connection
from django.core.management import call_command

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.etl.tests.integration.test_load_to_from_delta import load_delta_table_from_postgres, equal_datasets

from usaspending_api.transactions.delta_models.transaction_fabs import (
    TRANSACTION_FABS_COLUMN_INFO,
    TRANSACTION_FABS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    TRANSACTION_FPDS_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMNS,
)
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS
from usaspending_api.awards.delta_models.awards import AWARDS_COLUMNS

@fixture
def populate_initial_source_tables_pg():
    # Populate transactions.SourceAssistanceTransaction and associated broker.ExternalDataType data in Postgres
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=1,
        afa_generated_unique="award_assist_0001_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0001",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=2,
        afa_generated_unique="award_assist_0002_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=3,
        afa_generated_unique="award_assist_0002_trans_0002",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=4,
        afa_generated_unique="award_assist_0003_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0003",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=5,
        afa_generated_unique="award_assist_0003_trans_0002",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0003",
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_assistance_transaction",
        external_data_type_id=11,
        update_date="2022-10-31",
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-10-31", external_data_type=edt)

    # Populate transactions.SourceProcurementTransaction and associated broker.ExternalDataType data in Postgres
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=1,
        detached_award_proc_unique="award_procure_0001_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0001",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=2,
        detached_award_proc_unique="award_procure_0002_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=3,
        detached_award_proc_unique="award_procure_0002_trans_0002",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=4,
        detached_award_proc_unique="award_procure_0003_trans_0001",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0003",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=5,
        detached_award_proc_unique="award_procure_0003_trans_0002",
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0003",
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_procurement_transaction",
        external_data_type_id=10,
        update_date="2022-10-31",
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-10-31", external_data_type=edt)

    # Also need to populate values for int.transaction_[fabs|fpds|normalized], int.awards, and id lookup tables in
    # broker.ExternalData[Type|LoadDate] tables
    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt_tfpds = baker.make(
        "broker.ExternalDataType", name="transaction_fpds", external_data_type_id=201, update_date=None
    )
    edt_tfabs = baker.make(
        "broker.ExternalDataType", name="transaction_fabs", external_data_type_id=202, update_date=None
    )
    edt_tn = baker.make(
        "broker.ExternalDataType", name="transaction_normalized", external_data_type_id=203, update_date=None
    )
    edt_awards = baker.make(
        "broker.ExternalDataType", name="awards", external_data_type_id=204, update_date=None
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_tfpds)
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_tfabs)
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_tn)
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_awards)

    edt_tidlu = baker.make(
        "broker.ExternalDataType", name="transaction_id_lookup", external_data_type_id=205, update_date=None
    )
    edt_aidlu = baker.make(
        "broker.ExternalDataType", name="award_id_lookup", external_data_type_id=206, update_date=None
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_tidlu)
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_aidlu)

def execute_pg_transaction_loader():
    # Before calling the loader, make sure to set up the ExternalDataType and ExternalDataLoadDate entries for
    # the corresponding table
    edt = baker.make(
        "broker.ExternalDataType",
        name="fabs",
        external_data_type_id=2,
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt)
    call_command("fabs_nightly_loader", "--reload-all")

    edt = baker.make(
        "broker.ExternalDataType",
        name="fpds",
        external_data_type_id=1,
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt)
    call_command("load_fpds_transactions", "--reload-all")

@fixture
def populate_initial_other_tables_pg():
    # Need to create awards before creating transactions

    # Need to create entries in transaction_normalized before creating entries in transaction_f[ab|pd]s

    # There are no entries in broker.ExternalDataType for raw.transaction_normalized or raw.awards, so nothing
    # to do here for that.

    # Populate awards.TransactionFABS and associated broker.ExternalDataType data
    baker.make(
        "awards.TransactionFABS",
        transaction=assist_transactions[0],
        published_fabs_id=1,
        afa_generated_unique="award_assist_0001_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0001".upper(),
    )
    baker.make(
        "awards.TransactionFABS",
        transaction=assist_transactions[1],
        published_fabs_id=2,
        afa_generated_unique="award_assist_0002_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002".upper(),
    )
    baker.make(
        "awards.TransactionFABS",
        transaction=assist_transactions[2],
        published_fabs_id=3,
        afa_generated_unique="award_assist_0002_trans_0002".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002".upper(),
    )
    baker.make(
        "awards.TransactionFABS",
        transaction=assist_transactions[3],
        published_fabs_id=4,
        afa_generated_unique="award_assist_0003_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0003".upper(),
    )
    baker.make(
        "awards.TransactionFABS",
        transaction=assist_transactions[4],
        published_fabs_id=5,
        afa_generated_unique="award_assist_0003_trans_0002".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0003".upper(),
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="fabs",
        external_data_type_id=2,
        update_date="2022-11-01",
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-11-01", external_data_type=edt)

    # Populate awards.TransactionFPDS and associated broker.ExternalDataType data
    baker.make(
        "awards.TransactionFPDS",
        transaction=assist_transactions[0],
        detached_award_procurement_id=1,
        detached_award_proc_unique="award_procure_0001_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0001".upper(),
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=assist_transactions[1],
        detached_award_procurement_id=2,
        detached_award_proc_unique="award_procure_0002_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002".upper(),
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=assist_transactions[2],
        detached_award_procurement_id=3,
        detached_award_proc_unique="award_procure_0002_trans_0002".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002".upper(),
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=assist_transactions[3],
        detached_award_procurement_id=4,
        detached_award_proc_unique="award_procure_0003_trans_0001".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0003".upper(),
    )
    baker.make(
        "awards.TransactionFPDS",
        transaction=assist_transactions[4],
        detached_award_procurement_id=5,
        detached_award_proc_unique="award_procure_0003_trans_0002".upper(),
        action_date="2022-10-31",
        created_at=datetime(year=2022, month=10, day=31),
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0003".upper(),
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="fabs",
        external_data_type_id=2,
        update_date="2022-11-01",
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-11-01", external_data_type=edt)

@dataclass
class TableLoadInfo:
    spark: SparkSession
    table_name: str
    data: Sequence[Dict[str, Any]]
    overwrite: Optional[bool] = False

def load_to_raw_delta_table(spark, s3_data_bucket, table_name, data, overwrite = False):
    table_to_col_names_dict = {}
    table_to_col_names_dict["transaction_fabs"] = TRANSACTION_FABS_COLUMNS
    table_to_col_names_dict["transaction_fpds"] = TRANSACTION_FPDS_COLUMNS
    table_to_col_names_dict["transaction_normalized"] = list(TRANSACTION_NORMALIZED_COLUMNS)
    table_to_col_names_dict["awards"] = list(AWARDS_COLUMNS)

    table_to_col_info_dict = {}
    for tbl_name, col_info in zip(("transaction_fabs", "transaction_fpds"),
                                  (TRANSACTION_FABS_COLUMN_INFO, TRANSACTION_FPDS_COLUMN_INFO)):
        table_to_col_info_dict[tbl_name] = {}
        for col in col_info:
            table_to_col_info_dict[tbl_name][col.silver_name] = col

    # Make sure the table has been created first
    call_command("create_delta_table", "--destination-table", table_name, "--spark-s3-bucket", s3_data_bucket)

    if data:
        insert_sql = f"INSERT {'OVERWRITE' if overwrite else 'INTO'} raw.{table_name} VALUES\n"
        row_strs = []
        for row in data:
            value_strs =[]
            for col_name in table_to_col_names_dict[table_name]:
                value = row.get(col_name)
                if isinstance(value, (str, bytes)):
                    # Quote strings for insertion into DB
                    value_strs.append(f"'{value}'")
                elif isinstance(value, datetime):
                    # Convert to string and quote
                    value_strs.append(f"""'{value.isoformat(" ")}'""")
                elif isinstance(value, bool):
                    value_strs.append(str(value).upper())
                elif isinstance(value, (Sequence, Set)):
                    # Assume "sequences" must be "sequences" of strings, so quote each item in the "sequence"
                    value = [f"'{item}'" for item in value]
                    value_strs.append(f"ARRAY({', '.join(value)})")
                elif value is None:
                    col_info = table_to_col_info_dict.get(table_name)
                    if (col_info and col_info[col_name].delta_type.upper() == "BOOLEAN"
                            and not col_info[col_name].handling == "leave_null"):
                        # Convert None/NULL to false for boolean columns unless specified to leave the null
                        value_strs.append("FALSE")
                    else:
                        value_strs.append("NULL")
                else:
                    value_strs.append(str(value))

            row_strs.append(f"    ({', '.join(value_strs)})")

        sql = "".join([insert_sql, ",\n".join(row_strs), ";"])
        spark.sql(sql)

def load_tables_to_delta(
    s3_data_bucket, load_source_tables = True, load_other_raw_tables = None):
    if load_source_tables:
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        load_delta_table_from_postgres("detached_award_procurement", s3_data_bucket)

    if load_other_raw_tables:
        for item in load_other_raw_tables:
            if isinstance(item, TableLoadInfo):
                load_to_raw_delta_table(item.spark, s3_data_bucket, item.table_name, item.data, item.overwrite)
            else:
                load_delta_table_from_postgres(item, s3_data_bucket)

class TestInitialRun:
    @staticmethod
    def initial_run(
        s3_data_bucket, execute_pg_loader = True, load_source_tables = True, load_other_raw_tables = None,
            initial_copy = True
    ):
        if execute_pg_loader:
            execute_pg_transaction_loader()
        load_tables_to_delta(s3_data_bucket, load_source_tables, load_other_raw_tables)
        call_params = ["load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_data_bucket]
        if not initial_copy:
            call_params.append("--no-initial-copy")
        call_command(*call_params)

    @staticmethod
    def verify_transaction_ids(spark, expected_transaction_id_lookup):
        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Verify max transaction id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_transaction_id = cursor.fetchone()[0]
        if expected_transaction_id_lookup:
            assert max_transaction_id == max([transaction["id"] for transaction in expected_transaction_id_lookup])
        else:
            assert max_transaction_id == 1

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_transaction_id}, false)")

    @staticmethod
    def verify_award_ids(spark, expected_award_id_lookup):
        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        # Verify max award id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_award_id = cursor.fetchone()[0]
        if expected_award_id_lookup:
            assert max_award_id == max([award["id"] for award in expected_award_id_lookup])
        else:
            assert max_award_id == 1

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('award_id_seq', {max_award_id}, false)")

    @staticmethod
    def verify_lookup_info(spark, expected_transaction_id_lookup, expected_award_id_lookup):
        TestInitialRun.verify_transaction_ids(spark, expected_transaction_id_lookup)
        TestInitialRun.verify_award_ids(spark, expected_award_id_lookup)

    @staticmethod
    def verify_raw_vs_int_tables(spark, table_name, col_names):
        # Make sure the raw and int versions of the given table match
        result = spark.sql(f"""
                    SELECT {', '.join(col_names)} FROM int.{table_name} 
                    MINUS 
                    SELECT {', '.join(col_names)} FROM raw.{table_name}
                    """
                 ).collect()
        assert len(result) == 0

        result = spark.sql(f"""
                    SELECT {', '.join(col_names)} FROM raw.{table_name} 
                    MINUS 
                    SELECT {', '.join(col_names)} FROM int.{table_name}
                    """
                 ).collect()
        assert len(result) == 0

    @staticmethod
    def verify(
        spark, expected_transaction_id_lookup, expected_award_id_lookup, expected_normalized_count = 0,
            expected_fabs_count = 0, expected_fpds_count = 0, verify_raw_vs_int_tables = True
    ):
        TestInitialRun.verify_lookup_info(spark, expected_transaction_id_lookup, expected_award_id_lookup)

        # Make sure int.transaction_[normalized,fabs,fpds] tables have been created and have the expected sizes.
        for table_name, expected_count, col_names in zip(
                (f"transaction_{t}" for t in ("normalized", "fabs", "fpds")),
                (expected_normalized_count, expected_fabs_count, expected_fpds_count),
                (list(TRANSACTION_NORMALIZED_COLUMNS), TRANSACTION_FABS_COLUMNS, TRANSACTION_FPDS_COLUMNS)
        ):
            actual_count = spark.sql(f"SELECT COUNT(*) AS count from int.{table_name}").collect()[0]["count"]
            assert actual_count == expected_count
            if expected_count > 0 and verify_raw_vs_int_tables:
                TestInitialRun.verify_raw_vs_int_tables(spark, table_name, col_names)

    @mark.django_db(transaction=True)
    def test_edge_cases_using_only_source_tables(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Call initial run with no raw tables, except for published_fabs and detached_award_procurement.
        # Also, don't do initial copy of tables
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, initial_copy=False)
        TestInitialRun.verify(spark, [], [])

        # 2. Call initial run with existing, but empty raw.transaction_normalized and raw.awards tables

        # Make sure raw.transaction_normalized and raw.awards exist
        for table_name in ("transaction_normalized", "awards"):
            call_command(
                "create_delta_table", "--destination-table", table_name, "--spark-s3-bucket", s3_unittest_data_bucket
            )
        # Don't reload the source tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, False)
        TestInitialRun.verify(spark, [], [])

class TestInitialRunWithPostgresLoader:
    expected_initial_transaction_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
        },
        {
            "id": 3,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002".upper(),
        },
        {
            "id": 4,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
        },
        {
            "id": 7,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
        },
        {
            "id": 8,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002".upper(),
        },
        {
            "id": 9,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
        },
        {
            "id": 10,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002".upper(),
        },
    ]

    expected_initial_award_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0001".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0002".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002".upper(),
            "generated_unique_award_id": "award_assist_0002".upper(),
        },
        {
            "id": 3,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0003".upper(),
        },
        {
            "id": 3,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002".upper(),
            "generated_unique_award_id": "award_assist_0003".upper(),
        },
        {
            "id": 4,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0001".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0002".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002".upper(),
            "generated_unique_award_id": "award_procure_0002".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0003".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002".upper(),
            "generated_unique_award_id": "award_procure_0003".upper(),
        },
    ]

    expected_initial_transaction_fabs = [
        {
            "published_fabs_id": 1,
            "afa_generated_unique": "award_assist_0001_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 1,
            "unique_award_key": "award_assist_0001".upper(),
        },
        {
            "published_fabs_id": 2,
            "afa_generated_unique": "award_assist_0002_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 2,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "published_fabs_id": 3,
            "afa_generated_unique": "award_assist_0002_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 3,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "published_fabs_id": 4,
            "afa_generated_unique": "award_assist_0003_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 4,
            "unique_award_key": "award_assist_0003".upper(),
        },
        {
            "published_fabs_id": 5,
            "afa_generated_unique": "award_assist_0003_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 5,
            "unique_award_key": "award_assist_0003".upper(),
        },
    ]

    expected_initial_transaction_fpds = [
        {
            "detached_award_procurement_id": 1,
            "detached_award_proc_unique": "award_procure_0001_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 6,
            "unique_award_key": "award_procure_0001".upper(),
        },
        {
            "detached_award_procurement_id": 2,
            "detached_award_proc_unique": "award_procure_0002_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 7,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "detached_award_procurement_id": 3,
            "detached_award_proc_unique": "award_procure_0002_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 8,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "detached_award_procurement_id": 4,
            "detached_award_proc_unique": "award_procure_0003_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 9,
            "unique_award_key": "award_procure_0003".upper(),
        },
        {
            "detached_award_procurement_id": 5,
            "detached_award_proc_unique": "award_procure_0003_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 10,
            "unique_award_key": "award_procure_0003".upper(),
        },
    ]

    # Although transaction_normalized and source[assistance,procurement]_transaction django models say that
    # the unique_award_key can be NULL, actually trying to propagate a NULL in this field from one of the source
    # tables will kill the Postgres transaction loader, so only testing for NULLs in this field originating in Delta.
    @mark.django_db(transaction=True, reset_sequences=True)
    def test_nulls_in_trans_norm_unique_award_key_from_delta(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        execute_pg_transaction_loader()
        load_tables_to_delta(s3_unittest_data_bucket, load_other_raw_tables=["transaction_normalized"])

        spark.sql(
            """
            UPDATE raw.transaction_normalized
            SET unique_award_key = NULL
            WHERE id = 4
        """
        )

        with raises(ValueError, match="Found 1 NULL in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

        spark.sql(
            """
            UPDATE raw.transaction_normalized
            SET unique_award_key = NULL
            WHERE id = 5
        """
        )

        with raises(ValueError, match="Found 2 NULLs in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_happy_path_scenarios(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Call initial_run, but set initial-copy to False

        # When calling without initial load of int tables, only load essential tables to Delta
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, load_other_raw_tables=("transaction_normalized", "awards"), initial_copy=False
        )
        TestInitialRun.verify(spark, self.expected_initial_transaction_id_lookup, self.expected_initial_award_id_lookup)

        # 2. Call initial_run with initial-copy, but have empty raw.transaction_f[ab|pd]s tables
        destination_tables = ("transaction_fabs", "transaction_fpds")
        for destination_table in destination_tables:
            call_command(
                "create_delta_table",
                "--destination-table",
                destination_table,
                "--spark-s3-bucket",
                s3_unittest_data_bucket
            )
        # Don't call Postgres loader again or reload any of the tables
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, False)
        TestInitialRun.verify(
            spark, self.expected_initial_transaction_id_lookup, self.expected_initial_award_id_lookup, 10
        )

        # 3. Call initial_run with initial-copy, and have all raw tables populated
        # Also, don't call Postgres loader again or reload the source tables again
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, False, destination_tables)
        TestInitialRun.verify(
            spark, self.expected_initial_transaction_id_lookup, self.expected_initial_award_id_lookup, 10, 5, 5
        )


class TestInitialRunNoPostgresLoader:
    expected_initial_transaction_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
        },
        {
            "id": 3,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
        },
        {
            "id": 4,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002".upper(),
        },
        {
            "id": 7,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
        },
        {
            "id": 8,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002".upper(),
        },
        {
            "id": 9,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
        },
        {
            "id": 10,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002".upper(),
        },
    ]

    expected_initial_award_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0001".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0002".upper(),
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002".upper(),
            "generated_unique_award_id": "award_assist_0002".upper(),
        },
        {
            "id": 3,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0001".upper(),
        },
        {
            "id": 4,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0002".upper(),
        },
        {
            "id": 4,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002".upper(),
            "generated_unique_award_id": "award_procure_0002".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
            "generated_unique_award_id": "award_assist_0003".upper(),
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002".upper(),
            "generated_unique_award_id": "award_assist_0003".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
            "generated_unique_award_id": "award_procure_0003".upper(),
        },
        {
            "id": 6,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002".upper(),
            "generated_unique_award_id": "award_procure_0003".upper(),
        },
    ]

    initial_awards = [
        {
            "id": 1,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_assist_0001".upper(),
            "is_fpds": False,
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
            "subaward_count": 0
        },
        {
            "id": 2,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_assist_0002".upper(),
            "is_fpds": False,
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
            "subaward_count": 0
        },
        {
            "id": 3,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_procure_0001".upper(),
            "is_fpds": True,
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
            "subaward_count": 0
        },
        {
            "id": 4,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_procure_0002".upper(),
            "is_fpds": True,
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
            "subaward_count": 0
        },
        {
            "id": 5,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_assist_0003".upper(),
            "is_fpds": False,
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
            "subaward_count": 0
        },
        {
            "id": 6,
            "update_date": datetime(year=2022, month=11, day=1),
            "generated_unique_award_id": "award_procure_0003".upper(),
            "is_fpds": True,
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
            "subaward_count": 0
        }
    ]

    initial_transaction_normalized = [
        {
            "id": 1,
            "award_id": 1,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_assist_0001_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": False,
            "unique_award_key": "award_assist_0001".upper(),
        },
        {
            "id": 2,
            "award_id": 3,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_procure_0001_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": True,
            "unique_award_key": "award_procure_0001".upper(),
        },
        {
            "id": 3,
            "award_id": 2,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_assist_0002_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": False,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "id": 4,
            "award_id": 4,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_procure_0002_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": True,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "id": 5,
            "award_id": 2,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_assist_0002_trans_0002".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": False,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "id": 6,
            "award_id": 4,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_procure_0002_trans_0002".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": True,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "id": 7,
            "award_id": 5,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_assist_0003_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": False,
            "unique_award_key": "award_assist_0003".upper(),
        },
        {
            "id": 8,
            "award_id": 5,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_assist_0003_trans_0002".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": False,
            "unique_award_key": "award_assist_0003".upper(),
        },
        {
            "id": 9,
            "award_id": 6,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_procure_0003_trans_0001".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": True,
            "unique_award_key": "award_procure_0003".upper(),
        },
        {
            "id": 10,
            "award_id": 6,
            "business_categories": [],
            "action_date": datetime(year=2022, month=10, day=31),
            "create_date": datetime(year=2022, month=11, day=1),
            "transaction_unique_id": "award_procure_0003_trans_0002".upper(),
            "update_date": datetime(year=2022, month=11, day=1),
            "is_fpds": True,
            "unique_award_key": "award_procure_0003".upper(),
        },
    ]

    initial_transaction_fabs = [
        {
            "published_fabs_id": 1,
            "afa_generated_unique": "award_assist_0001_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 1,
            "unique_award_key": "award_assist_0001".upper(),
        },
        {
            "published_fabs_id": 2,
            "afa_generated_unique": "award_assist_0002_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 3,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "published_fabs_id": 3,
            "afa_generated_unique": "award_assist_0002_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 5,
            "unique_award_key": "award_assist_0002".upper(),
        },
        {
            "published_fabs_id": 4,
            "afa_generated_unique": "award_assist_0003_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 7,
            "unique_award_key": "award_assist_0003".upper(),
        },
        {
            "published_fabs_id": 5,
            "afa_generated_unique": "award_assist_0003_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "is_active": True,
            "transaction_id": 8,
            "unique_award_key": "award_assist_0003".upper(),
        },
    ]

    initial_transaction_fpds = [
        {
            "detached_award_procurement_id": 1,
            "detached_award_proc_unique": "award_procure_0001_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 2,
            "unique_award_key": "award_procure_0001".upper(),
        },
        {
            "detached_award_procurement_id": 2,
            "detached_award_proc_unique": "award_procure_0002_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 4,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "detached_award_procurement_id": 3,
            "detached_award_proc_unique": "award_procure_0002_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 6,
            "unique_award_key": "award_procure_0002".upper(),
        },
        {
            "detached_award_procurement_id": 4,
            "detached_award_proc_unique": "award_procure_0003_trans_0001".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 9,
            "unique_award_key": "award_procure_0003".upper(),
        },
        {
            "detached_award_procurement_id": 5,
            "detached_award_proc_unique": "award_procure_0003_trans_0002".upper(),
            "action_date": "2022-10-31",
            "created_at": datetime(year=2022, month=10, day=31),
            "updated_at": datetime(year=2022, month=10, day=31),
            "transaction_id": 10,
            "unique_award_key": "award_procure_0003".upper(),
        },
    ]

    # This test will only load the source tables from postgres, and NOT use the Postgres transaction loader
    # to populate any other Delta tables, so can only test for NULLs originating in Delta.
    @mark.django_db(transaction=True)
    def test_nulls_in_trans_norm_unique_award_key_from_delta(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # Only load the source tables from Postgres.
        load_tables_to_delta(s3_unittest_data_bucket)

        # Directly load the contents of raw.transaction_normalized to Delta
        load_to_raw_delta_table(
            spark, s3_unittest_data_bucket, "transaction_normalized", self.initial_transaction_normalized
        )

        spark.sql(
            """
            UPDATE raw.transaction_normalized
            SET unique_award_key = NULL
            WHERE id = 5
            """
        )

        with raises(ValueError, match="Found 1 NULL in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

        spark.sql(
            """
            UPDATE raw.transaction_normalized
            SET unique_award_key = NULL
            WHERE id = 6
            """
        )

        with raises(ValueError, match="Found 2 NULLs in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

    @mark.django_db(transaction=True)
    def test_happy_path_scenarios(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Call initial_run, but set no-initial-copy
        
        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(spark, "transaction_normalized", self.initial_transaction_normalized),
            TableLoadInfo(spark, "awards", self.initial_awards)
        ]
        # Don't call Postgres loader, though.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, False, load_other_raw_tables=load_other_raw_tables, initial_copy=False
        )
        TestInitialRun.verify(spark, self.expected_initial_transaction_id_lookup, self.expected_initial_award_id_lookup)

        # 2. Call initial_run with initial-copy, but have empty raw.transaction_f[ab|pd]s tables
        destination_tables = ("transaction_fabs", "transaction_fpds")
        for destination_table in destination_tables:
            call_command(
                "create_delta_table",
                "--destination-table",
                destination_table,
                "--spark-s3-bucket",
                s3_unittest_data_bucket
            )
        # Don't call Postgres loader or reload any of the tables
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, False)
        TestInitialRun.verify(
            spark,
            self.expected_initial_transaction_id_lookup,
            self.expected_initial_award_id_lookup,
            len(self.initial_transaction_normalized)
        )

        # 3. Call initial_run with initial-copy, and have all raw tables populated

        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(spark, "transaction_fabs", self.initial_transaction_fabs),
            TableLoadInfo(spark, "transaction_fpds", self.initial_transaction_fpds)
        ]
        # Don't call Postgres loader or re-load the source tables, though.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, False, False, load_other_raw_tables
        )
        TestInitialRun.verify(
            spark,
            self.expected_initial_transaction_id_lookup,
            self.expected_initial_award_id_lookup,
            len(self.initial_transaction_normalized),
            len(self.initial_transaction_fabs),
            len(self.initial_transaction_fpds),
        )


class TestTransactionIdLookup:
    @mark.django_db(transaction=True)
    def test_unexpected_paths(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with etl-level of transaction_id_lookup without first
        # calling calling load_transactions_in_delta with etl-level of initial_run

        # First, load the source tables to Delta.
        load_tables_to_delta(s3_unittest_data_bucket)

        with raises(pyspark.sql.utils.AnalysisException, match="Table or view not found: int.transaction_id_lookup"):
            call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # 2. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then transaction_id_lookup.  However, call initial_run with blank raw.transaction_normalized
        # and raw.awards tables.

        # First, create blank raw.transaction_normalized and raw.awards tables
        for table_name in ("transaction_normalized", "awards"):
            call_command(
                "create_delta_table", "--destination-table", table_name, "--spark-s3-bucket", s3_unittest_data_bucket
            )

        # Then, call load_transactions_in_delta with etl-level of initial_run and verify.
        # Don't reload the source tables, and don't do initial copy of transaction tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, initial_copy=False)
        TestInitialRun.verify(spark, [], [])

        # Then, call load_transactions_in_delta with etl-level of transaction_id_lookup.
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # The expected transaction_id_lookup table should be the same as in TestInitialRunWithPostgresLoader,
        # but all of the transaction ids should be 1 larger than expected there.
        expected_transaction_id_lookup = deepcopy(
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup
        )
        for item in expected_transaction_id_lookup:
            item["id"] += 1
        TestInitialRun.verify(spark, expected_transaction_id_lookup, [])

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("transaction_id_lookup")
        assert last_load_dt.date() == date.today()

    def happy_path_shared_tests(self, spark, s3_data_bucket, expected_transaction_id_lookup):
        # 2. Test deleting the transaction(s) with the last transaction ID(s) from the appropriate raw table,
        # followed by a call to load_transaction_in_delta with etl-level of transaction_id_lookup
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 4 OR detached_award_procurement_id = 5
        """
        )
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup.pop()
        expected_transaction_id_lookup.pop()
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Also, make sure transaction_id_seq hasn't gone backwards
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_transaction_id = cursor.fetchone()[0]
        assert max_transaction_id == 10

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value as previously.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_transaction_id}, false)")

        # 3. Test for a single inserted transaction, and another call to load_transaction_in_delta with etl-level of
        # transaction_id_lookup.

        # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
        # Postgres table, and then push the updated table to Delta.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=6,
            afa_generated_unique="award_assist_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            is_active=True,
            unique_award_key="award_assist_0004",
        )
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup.append(
            {
                "id": 11,
                "detached_award_procurement_id": None,
                "published_fabs_id": 6,
                "transaction_unique_id": "award_assist_0004_trans_0001".upper(),
            }
        )
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        return expected_transaction_id_lookup

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_happy_path_scenarios_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then transaction_id_lookup

        # Since these tests only care about the condition of the transaction_id_lookup table after various
        # operations, only load the essential tables to Delta, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, load_other_raw_tables=("transaction_normalized", "awards"), initial_copy=False
        )
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # With no deletes or inserts yet, the transaction_id_lookup table should be the same as after the initial run
        TestInitialRun.verify(
            spark,
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunWithPostgresLoader.expected_initial_award_id_lookup
        )

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("transaction_id_lookup")
        assert last_load_dt.date() == date.today()

        # Test a couple of shared items
        expected_transaction_id_lookup = self.happy_path_shared_tests(
            spark,
            s3_unittest_data_bucket,
            deepcopy(TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup)
        )

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # transaction_id_lookup, and test that the results are as expected.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

        spark.sql(
            """
            DELETE FROM raw.published_fabs
            WHERE published_fabs_id = 2 OR published_fabs_id = 3
        """
        )
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 1
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(3)
        expected_transaction_id_lookup.append(
            {
                "id": 12,
                "detached_award_procurement_id": 6,
                "published_fabs_id": None,
                "transaction_unique_id": "award_procure_0004_trans_0001".upper(),
            }
        )
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

    @mark.django_db(transaction=True)
    def test_happy_path_scenarios_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then transaction_id_lookup

        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(
                spark, "transaction_normalized", TestInitialRunNoPostgresLoader.initial_transaction_normalized
            ),
            TableLoadInfo(spark, "awards", TestInitialRunNoPostgresLoader.initial_awards)
        ]

        # Don't call the Postgres loader, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, False, load_other_raw_tables=load_other_raw_tables, initial_copy=False
        )
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # With no deletes or inserts yet, the transaction_id_lookup table should be the same as after
        # the initial run
        TestInitialRun.verify(
            spark,
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup
        )

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("transaction_id_lookup")
        assert last_load_dt.date() == date.today()

        # Test a couple of shared items
        expected_transaction_id_lookup = self.happy_path_shared_tests(
            spark,
            s3_unittest_data_bucket,
            deepcopy(TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup)
        )

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # transaction_id_lookup, and test that the results are as expected.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

        spark.sql(
            """
            DELETE FROM raw.published_fabs
            WHERE published_fabs_id = 2 OR published_fabs_id = 3
        """
        )
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 1
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(2)
        expected_transaction_id_lookup.append(
            {
                "id": 12,
                "detached_award_procurement_id": 6,
                "published_fabs_id": None,
                "transaction_unique_id": "award_procure_0004_trans_0001".upper(),
            }
        )
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")


class TestAwardIdLookup:
    @mark.django_db(transaction=True)
    def test_unexpected_paths(
            self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with etl-level of award_id_lookup without first
        # calling calling load_transactions_in_delta with etl-level of initial_run

        # First, load the source tables to Delta
        load_tables_to_delta(s3_unittest_data_bucket)

        with raises(pyspark.sql.utils.AnalysisException, match="Table or view not found: int.award_id_lookup"):
            call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # 2. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then award_id_lookup.  However, call initial_run with blank raw.transaction_normalized
        # and raw.awards tables.

        # First, create blank raw.transaction_normalized and raw.awards tables
        for table_name in ("transaction_normalized", "awards"):
            call_command(
                "create_delta_table", "--destination-table", table_name, "--spark-s3-bucket",
                s3_unittest_data_bucket
            )

        # Then, call load_transactions_in_delta with etl-level of initial_run and verify.
        # Don't reload the source tables, and don't do initial copy of transaction tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, initial_copy=False)
        TestInitialRun.verify(spark, [], [])

        # Then, call load_transactions_in_delta with etl-level of transaction_id_lookup.
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # The expected award_id_lookup table should be the same as in TestInitialRunWithPostgresLoader,
        # but all of the award ids should be 1 larger than expected there.
        expected_award_id_lookup = deepcopy(TestInitialRunWithPostgresLoader.expected_initial_award_id_lookup)
        for item in expected_award_id_lookup:
            item["id"] += 1
        TestInitialRun.verify(spark, [], expected_award_id_lookup)

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("award_id_lookup")
        assert last_load_dt.date() == date.today()

    def happy_path_shared_tests(self, spark, s3_data_bucket, expected_award_id_lookup):
        # 2. Test deleting the transaction(s) with the last award ID(s) from the appropriate raw table,
        # followed by a call to load_transaction_in_delta with etl-level of award_id_lookup
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 4 OR detached_award_procurement_id = 5
        """
        )
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup.pop()
        expected_award_id_lookup.pop()
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        # Also, make sure award_id_seq hasn't gone backwards
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_award_id = cursor.fetchone()[0]
        assert max_award_id == 6

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value as previously.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('award_id_seq', {max_award_id}, false)")

        # 3. Test for a single inserted transaction, and another call to load_transaction_in_delta with etl-level of
        # award_id_lookup.

        # Can't use spark.sql to just insert rows with only values for desired columns (need to specify values for
        # all of them), so using model baker to add new rows to Postgres table, and then pushing new table to Delta.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=6,
            afa_generated_unique="award_assist_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            is_active=True,
            unique_award_key="award_assist_0004",
        )
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup.append(
            {
                "id": 7,
                "detached_award_procurement_id": None,
                "published_fabs_id": 6,
                "transaction_unique_id": "award_assist_0004_trans_0001".upper(),
                "generated_unique_award_id": "award_assist_0004".upper(),
            }
        )
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        return expected_award_id_lookup

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_happy_path_scenarios_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then award_id_lookup

        # Since these tests only care about the condition of the transaction_id_lookup table after various
        # operations, only load the essential tables to Delta, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, load_other_raw_tables=("transaction_normalized", "awards"), initial_copy=False
        )
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # With no deletes or inserts, the award_id_lookup table should be the same as after the initial run
        TestInitialRun.verify(
            spark,
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunWithPostgresLoader.expected_initial_award_id_lookup
        )

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("award_id_lookup")
        assert last_load_dt.date() == date.today()

        # Test a couple of shared items
        expected_award_id_lookup = self.happy_path_shared_tests(
            spark,
            s3_unittest_data_bucket,
            deepcopy(TestInitialRunWithPostgresLoader.expected_initial_award_id_lookup)
        )

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # award_id_lookup, and test that the results are as expected.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

        spark.sql(
            """
            DELETE FROM raw.published_fabs
            WHERE published_fabs_id = 2 OR published_fabs_id = 3
        """
        )
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 1
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.pop(3)
        expected_award_id_lookup.append(
            {
                "id": 8,
                "detached_award_procurement_id": 6,
                "published_fabs_id": None,
                "transaction_unique_id": "award_procure_0004_trans_0001".upper(),
                "generated_unique_award_id": "award_procure_0004".upper(),
            }
        )
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

    @mark.django_db(transaction=True)
    def test_happy_path_scenarios_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then award_id_lookup

        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(
                spark, "transaction_normalized", TestInitialRunNoPostgresLoader.initial_transaction_normalized
            ),
            TableLoadInfo(spark, "awards", TestInitialRunNoPostgresLoader.initial_awards)
        ]

        # Don't call the Postgres loader, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, False, load_other_raw_tables=load_other_raw_tables, initial_copy=False
        )
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # With no deletes or inserts, the award_id_lookup table should be the same as after the initial run
        TestInitialRun.verify(
            spark,
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup
        )

        # Verify last load date, as closely as we can (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("award_id_lookup")
        assert last_load_dt.date() == date.today()

        # Test a couple of shared items
        expected_award_id_lookup = self.happy_path_shared_tests(
            spark,
            s3_unittest_data_bucket,
            deepcopy(TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup)
        )

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # award_id_lookup, and test that the results are as expected.
        insert_datetime = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            action_date=insert_datetime.date().isoformat(),
            created_at=insert_datetime,
            updated_at=insert_datetime,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

        spark.sql(
            """
            DELETE FROM raw.published_fabs
            WHERE published_fabs_id = 2 OR published_fabs_id = 3
        """
        )
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 1
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup.pop(3)
        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.append(
            {
                "id": 8,
                "detached_award_procurement_id": 6,
                "published_fabs_id": None,
                "transaction_unique_id": "award_procure_0004_trans_0001".upper(),
                "generated_unique_award_id": "award_procure_0004".upper(),
            }
        )
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

class TransactionFabsFpdsCore:

    new_transaction_fabs_fpds_id = 6
    new_transaction_id = 11

    def __init__(
        self, spark, s3_data_bucket, etl_level, pk_field, compare_fields, source_table_name, baker_table,
            baker_kwargs, expected_initial_transaction_fabs, expected_initial_transaction_fpds
    ):
        self.spark = spark
        self.s3_data_bucket = s3_data_bucket
        self.etl_level = etl_level
        self.pk_field = pk_field
        self.source_table_name = source_table_name
        self.baker_table = baker_table
        self.compare_fields = compare_fields
        self.baker_kwargs = baker_kwargs
        self.expected_initial_transaction_fabs = expected_initial_transaction_fabs
        self.expected_initial_transaction_fpds = expected_initial_transaction_fpds

    def unexpected_paths_source_tables_only_test_core(self):
        # First, load the source tables to Delta
        load_tables_to_delta(self.s3_data_bucket)

        # 1. Test calling load_transactions_in_delta with etl-level of transaction_f[ab|pd]s before calling with
        # etl-level of initial_run.
        with raises(pyspark.sql.utils.AnalysisException, match=f"Table or view not found: int.{self.etl_level}"):
            call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # 2. Call load_transactions_in_delta with etl-level of initial_run first, but without first loading
        # raw.transaction_normalized or raw.awards.  Then immediately call load_transactions_in_delta with
        # etl-level of transaction_f[ab|pd]s.
        TestInitialRun.initial_run(self.s3_data_bucket, False)
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the transaction and award id lookup tables and other int transaction tables.  They should all be empty.
        TestInitialRun.verify(self.spark, [], [])

        # 3. With raw.transaction_normalized and raw.awards still not created, call load_transactions_in_delta
        # with etl-level of transaction_id_lookup, and then again with etl-level of transaction_f[ab|pd]s.

        # Since the call to load_transactions_in_delta with etl-level of transaction_f[ab|pd]s above succeeded, we first
        # need to reset the last load date on transaction_fabs
        update_last_load_date(self.etl_level, datetime(1970, 1, 1))

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # The expected transaction_id_lookup table should be the same as in TestInitialRunWithPostgresLoader,
        # but all of the transaction ids should be 1 larger than expected there.
        expected_transaction_id_lookup = deepcopy(
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup
        )
        for item in expected_transaction_id_lookup:
            item["id"] += 1
        TestInitialRun.verify(
            self.spark,
            expected_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            False
        )

        # Verify key fields in transaction_fabs table.  Note that the transaction_ids should be 1 more than in those
        # from TestInitialRunWithPostgresLoader
        query = f"SELECT {', '.join(self.compare_fields)} FROM int.{self.etl_level} ORDER BY {self.pk_field}"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]

        if len(self.expected_initial_transaction_fabs) > 0:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fabs)
        else:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fpds)
        for item in expected_transaction_fabs_fpds:
            item["transaction_id"] += 1
        assert equal_datasets(expected_transaction_fabs_fpds, delta_data, "")

    def unexpected_paths_test_core(
        self, execute_pg_loader, load_other_raw_tables, expected_initial_transaction_id_lookup,
    ):
        # 1. Call load_transactions_in_delta with etl-level of initial_run first, making sure to load
        # raw.transaction_normalized along with the source tables, but don't copy the raw tables to int.
        # Then immediately call load_transactions_in_delta with etl-level of transaction_f[ab|pd]s.
        TestInitialRun.initial_run(
            self.s3_data_bucket, execute_pg_loader, load_other_raw_tables=load_other_raw_tables, initial_copy=False
        )
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Even without the call to load_transactions_in_delta with etl-level of transaction_id_lookup, the appropriate
        # data will be populated in the transaction_id_lookup table via initial_run to allow the call to
        # load_transactions_in_delta with etl-level of transaction_fabs to populate int.transaction_fabs correctly with
        # the initial data.
        TestInitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            False
        )

        # Verify key fields in transaction_fabs table.
        query = f"SELECT {', '.join(self.compare_fields)} FROM int.{self.etl_level} ORDER BY {self.pk_field}"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]
        if len(self.expected_initial_transaction_fabs) > 0:
            assert equal_datasets(self.expected_initial_transaction_fabs, delta_data, "")
        else:
            assert equal_datasets(self.expected_initial_transaction_fpds, delta_data, "")

        # 2. Test inserting, updating, and deleting without calling load_transactions_in_delta with etl-level
        # of transaction_id_lookup before calling load_transactions_in_delta with etl-level of transaction_f[ab|pd]s.

        # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
        # Postgres table, and then push the updated table to Delta.
        insert_update_datetime = datetime.now(timezone.utc)
        self.baker_kwargs.update(
            {
                "action_date": insert_update_datetime.date().isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime
            }
        )
        baker.make(self.baker_table, **self.baker_kwargs)
        load_delta_table_from_postgres(self.source_table_name, self.s3_data_bucket)

        self.spark.sql(
            f"""
                UPDATE raw.{self.source_table_name}
                SET updated_at = '{insert_update_datetime}'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                DELETE FROM raw.{self.source_table_name}
                WHERE {self.pk_field} = 2 OR {self.pk_field} = 3
            """
        )

        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the transaction and award id lookup tables.  Without a call to load_transactions_in_delta with an
        # --etl-level of transaction_id_lookup or award_id_lookup, they should be the same as during the initial run.
        TestInitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            False
        )

        # Verify key fields in transaction_f[ab|pd]s table
        query = f"SELECT {', '.join(self.compare_fields)} FROM int.{self.etl_level} ORDER BY {self.pk_field}"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]

        # With no call to load_transactions_in_delta with etl-level of transaction_id_lookup, the above call to
        # load_transactions_in_delta with etl-level of transaction_f[ab|pd]s *should* pick up the *updates* in the
        # published f[ab|pd]s table because those transactions already exist in the transaction_id_lookup table.
        # However, this call should *NOT* pick up the inserts or deletes, since those transactions will not
        # have changed in the transaction_id_lookup table.
        if len(self.expected_initial_transaction_fabs) > 0:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fabs)
        else:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fpds)
        expected_transaction_fabs_fpds[-2]["updated_at"] = insert_update_datetime
        expected_transaction_fabs_fpds[-1]["updated_at"] = insert_update_datetime
        assert equal_datasets(expected_transaction_fabs_fpds, delta_data, "")

    @staticmethod
    def test_unexpected_paths_with_pg_loader(transaction_fabs_fpds_core):
        transaction_fabs_fpds_core.unexpected_paths_test_core(
            True,
            ["transaction_normalized"],
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup
        )

    @staticmethod
    def test_unexpected_paths_no_pg_loader(transaction_fabs_fpds_core):
        transaction_fabs_fpds_core.unexpected_paths_test_core(
            False,
            [
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    "transaction_normalized",
                    TestInitialRunNoPostgresLoader.initial_transaction_normalized
                )
            ],
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup
        )

    def happy_paths_test_core(
        self, execute_pg_loader, load_other_raw_tables, expected_initial_transaction_id_lookup,
            expected_initial_award_id_lookup, expected_transaction_id_lookup_pops,
            expected_transaction_id_lookup_append, expected_transaction_fabs_fpds_append
    ):
        # 1, Test calling load_transactions_in_delta with etl-level of transaction_f[ab|pd]s after calling with
        # etl-levels of initial_run and transaction_id_lookup.
        TestInitialRun.initial_run(
            self.s3_data_bucket, execute_pg_loader, load_other_raw_tables=load_other_raw_tables
        )
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the tables.  The transaction and award id lookup tables should be the same as during the initial run.
        # The transaction_normalized and transaction_f[ab|pd]s tables should have been copied from raw to int.
        TestInitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            expected_initial_award_id_lookup,
            len(expected_initial_transaction_id_lookup),
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds)
        )

        # Verify key fields in transaction_fabs table
        transaction_fabs_fpds_query = (
            f"SELECT {', '.join(self.compare_fields)} FROM int.{self.etl_level} ORDER BY {self.pk_field}"
        )
        delta_data = [row.asDict() for row in self.spark.sql(transaction_fabs_fpds_query).collect()]
        if len(self.expected_initial_transaction_fabs) > 0:
            assert equal_datasets(self.expected_initial_transaction_fabs, delta_data, "")
        else:
            assert equal_datasets(self.expected_initial_transaction_fpds, delta_data, "")

        # 2. Test inserting, updating, and deleting records followed by calling load_transactions_in_delta with
        # etl-levels of transaction_id_lookup and then transaction_f[ab|pd]s.

        # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
        # Postgres table, and then push the updated table to Delta.
        insert_update_datetime = datetime.now(timezone.utc)
        self.baker_kwargs.update(
            {
                "action_date": insert_update_datetime.date().isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime,
            }
        )
        baker.make(self.baker_table, **self.baker_kwargs)
        load_delta_table_from_postgres(self.source_table_name, self.s3_data_bucket)

        self.spark.sql(
            f"""
                UPDATE raw.{self.source_table_name}
                SET updated_at = '{insert_update_datetime}'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                DELETE FROM raw.{self.source_table_name}
                WHERE {self.pk_field} = 2 OR {self.pk_field} = 3
            """
        )

        # Need to load changes into the transaction_id_lookup table.
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]

        expected_transaction_id_lookup = deepcopy(expected_initial_transaction_id_lookup)
        expected_transaction_id_lookup.pop(expected_transaction_id_lookup_pops[0])
        expected_transaction_id_lookup.pop(expected_transaction_id_lookup_pops[1])
        expected_transaction_id_lookup_append.update(
            {
                "id": self.new_transaction_id,
            }
        )
        expected_transaction_id_lookup.append(expected_transaction_id_lookup_append)
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Verify key fields in transaction_f[ab|pd]s table
        delta_data = [row.asDict() for row in self.spark.sql(transaction_fabs_fpds_query).collect()]

        if len(self.expected_initial_transaction_fabs) > 0:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fabs)
        else:
            expected_transaction_fabs_fpds = deepcopy(self.expected_initial_transaction_fpds)
        expected_transaction_fabs_fpds.pop(1)
        expected_transaction_fabs_fpds.pop(1)
        expected_transaction_fabs_fpds[-2]["updated_at"] = insert_update_datetime
        expected_transaction_fabs_fpds[-1]["updated_at"] = insert_update_datetime
        expected_transaction_fabs_fpds_append.update(
            {
                "transaction_id": self.new_transaction_id,
                "action_date": insert_update_datetime.date().isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime,
            }
        )
        expected_transaction_fabs_fpds.append(expected_transaction_fabs_fpds_append)
        assert equal_datasets(expected_transaction_fabs_fpds, delta_data, "")

    @staticmethod
    def test_happy_paths_with_pg_loader(
        transaction_fabs_fpds_core, expected_transaction_id_lookup_pops, expected_transaction_id_lookup_append,
            expected_transaction_fabs_fpds_append
    ):
        transaction_fabs_fpds_core.happy_paths_test_core(
            True,
            ("transaction_normalized", transaction_fabs_fpds_core.etl_level, "awards"),
            TestInitialRunWithPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunWithPostgresLoader.expected_initial_award_id_lookup,
            expected_transaction_id_lookup_pops,
            expected_transaction_id_lookup_append,
            expected_transaction_fabs_fpds_append
        )

    @staticmethod
    def test_happy_paths_no_pg_loader(
        transaction_fabs_fpds_core, initial_transaction_fabs_fpds, expected_transaction_id_lookup_pops,
            expected_transaction_id_lookup_append, expected_transaction_fabs_fpds_append
    ):
        transaction_fabs_fpds_core.happy_paths_test_core(
            False,
            (
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    "transaction_normalized",
                    TestInitialRunNoPostgresLoader.initial_transaction_normalized
                ),
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    transaction_fabs_fpds_core.etl_level,
                    initial_transaction_fabs_fpds),
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    "awards",
                    TestInitialRunNoPostgresLoader.initial_awards),
            ),
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup,
            expected_transaction_id_lookup_pops,
            expected_transaction_id_lookup_append,
            expected_transaction_fabs_fpds_append
        )

class TestTransactionFabs():

    etl_level = "transaction_fabs"
    pk_field = "published_fabs_id"
    source_table_name = "published_fabs"
    baker_table = "transactions.SourceAssistanceTransaction"
    compare_fields = TestInitialRunWithPostgresLoader.expected_initial_transaction_fabs[0].keys()
    new_afa_generated_unique = "award_assist_0004_trans_0001"
    new_unique_award_key = "award_assist_0004"
    baker_kwargs = {
        "published_fabs_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "afa_generated_unique": new_afa_generated_unique,
        "is_active": True,
        "unique_award_key": new_unique_award_key
    }
    expected_transaction_id_lookup_append = {
        "detached_award_procurement_id": None,
        "published_fabs_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "transaction_unique_id": new_afa_generated_unique.upper(),
    }
    expected_transaction_fabs_fpds_append = {
        "afa_generated_unique": new_afa_generated_unique.upper(),
        "is_active": True,
        "published_fabs_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "unique_award_key": new_unique_award_key.upper(),
    }

    def generate_transaction_fabs_fpds_core(self, spark, s3_data_bucket, expected_initial_transaction_fabs):
        return TransactionFabsFpdsCore(
            spark,
            s3_data_bucket,
            self.etl_level,
            self.pk_field,
            self.compare_fields,
            self.source_table_name,
            self.baker_table,
            deepcopy(self.baker_kwargs),
            expected_initial_transaction_fabs,
            []
        )

    @mark.django_db(transaction=True)
    def test_unexpected_paths_source_tables_only(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self.generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fabs
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_unexpected_paths_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_unexpected_paths_with_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fabs
            )
        )

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_unexpected_paths_no_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fabs
            )
        )

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_happy_paths_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_happy_paths_with_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fabs
            ),
            (1, 1),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append
        )

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_happy_paths_no_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fabs
            ),
            TestInitialRunNoPostgresLoader.initial_transaction_fabs,
            (2, 3),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append
        )

class TestTransactionFpds:

    etl_level = "transaction_fpds"
    pk_field = "detached_award_procurement_id"
    source_table_name = "detached_award_procurement"
    baker_table = "transactions.SourceProcurementTransaction"
    compare_fields = TestInitialRunWithPostgresLoader.expected_initial_transaction_fpds[0].keys()
    new_detached_award_proc_unique = "award_procure_0004_trans_0001"
    new_unique_award_key = "award_procure_0004"
    baker_kwargs = {
        "detached_award_procurement_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "detached_award_proc_unique": new_detached_award_proc_unique,
        "unique_award_key": new_unique_award_key
    }
    expected_transaction_id_lookup_append = {
        "detached_award_procurement_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "published_fabs_id": None,
        "transaction_unique_id": new_detached_award_proc_unique.upper(),
    }
    expected_transaction_fabs_fpds_append = {
        "detached_award_proc_unique": new_detached_award_proc_unique.upper(),
        "detached_award_procurement_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "unique_award_key": new_unique_award_key.upper(),
    }

    def generate_transaction_fabs_fpds_core(self, spark, s3_data_bucket, expected_initial_transaction_fpds):
        return TransactionFabsFpdsCore(
            spark,
            s3_data_bucket,
            self.etl_level,
            self.pk_field,
            self.compare_fields,
            self.source_table_name,
            self.baker_table,
            deepcopy(self.baker_kwargs),
            [],
            expected_initial_transaction_fpds,
        )

    @mark.django_db(transaction=True)
    def test_unexpected_paths_source_tables_only(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self.generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fpds
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_unexpected_paths_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_unexpected_paths_with_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fpds
            )
        )

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_unexpected_paths_no_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fpds
            )
        )

    @mark.django_db(transaction=True, reset_sequences=True)
    def test_happy_paths_with_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_happy_paths_with_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunWithPostgresLoader.expected_initial_transaction_fpds
            ),
            (6, 6),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append
        )

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.test_happy_paths_no_pg_loader(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fpds
            ),
            TestInitialRunNoPostgresLoader.initial_transaction_fpds,
            (3, 4),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append
        )

