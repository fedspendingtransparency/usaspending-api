"""Automated Unit Tests for the the loading of transaction and award tables in Delta Lake.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import dateutil
import re
import pyspark

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from django.db import connection
from django.core.management import call_command
from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import fixture, mark, raises
from typing import Any, Dict, Optional, Sequence

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.helpers.spark_helpers import load_dict_to_delta_table
from usaspending_api.etl.tests.integration.test_load_to_from_delta import load_delta_table_from_postgres, equal_datasets
from usaspending_api.transactions.delta_models.transaction_fabs import TRANSACTION_FABS_COLUMNS
from usaspending_api.transactions.delta_models.transaction_fpds import TRANSACTION_FPDS_COLUMNS
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS

BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
initial_datetime = datetime(2022, 10, 31, tzinfo=timezone.utc)
initial_source_table_load_datetime = initial_datetime + timedelta(hours=12)

initial_assists = [
    {
        "published_fabs_id": 1,
        "afa_generated_unique": "award_assist_0001_trans_0001",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "is_active": True,
        "unique_award_key": "award_assist_0001",
    },
    {
        "published_fabs_id": 2,
        "afa_generated_unique": "award_assist_0002_trans_0001",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "is_active": True,
        "unique_award_key": "award_assist_0002",
    },
    {
        "published_fabs_id": 3,
        "afa_generated_unique": "award_assist_0002_trans_0002",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": initial_datetime.strftime("%Y%m%d"),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "is_active": True,
        "unique_award_key": "award_assist_0002",
    },
    {
        "published_fabs_id": 4,
        "afa_generated_unique": "award_assist_0003_trans_0001",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": initial_datetime.strftime("%Y%m%d"),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "is_active": True,
        "unique_award_key": "award_assist_0003",
    },
    {
        "published_fabs_id": 5,
        "afa_generated_unique": "award_assist_0003_trans_0002",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "is_active": True,
        "unique_award_key": "award_assist_0003",
    },
]

initial_procures = [
    {
        "detached_award_procurement_id": 1,
        "detached_award_proc_unique": "award_procure_0001_trans_0001",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "unique_award_key": "award_procure_0001",
    },
    {
        "detached_award_procurement_id": 2,
        "detached_award_proc_unique": "award_procure_0002_trans_0001",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "unique_award_key": "award_procure_0002",
    },
    {
        "detached_award_procurement_id": 3,
        "detached_award_proc_unique": "award_procure_0002_trans_0002",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": initial_datetime.strftime("%Y%m%d"),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "unique_award_key": "award_procure_0002",
    },
    {
        "detached_award_procurement_id": 4,
        "detached_award_proc_unique": "award_procure_0003_trans_0001",
        # Deliberately formatting this action_date somewhat unusually.
        "action_date": initial_datetime.strftime("%Y%m%d"),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "unique_award_key": "award_procure_0003",
    },
    {
        "detached_award_procurement_id": 5,
        "detached_award_proc_unique": "award_procure_0003_trans_0002",
        "action_date": initial_datetime.isoformat(),
        "created_at": initial_datetime,
        "updated_at": initial_datetime,
        "unique_award_key": "award_procure_0003",
    },
]

new_assist = {
    "published_fabs_id": 6,
    "afa_generated_unique": "award_assist_0004_trans_0001",
    "is_active": True,
    "unique_award_key": "award_assist_0004",
}

new_procure = {
    "detached_award_procurement_id": 6,
    "detached_award_proc_unique": "award_procure_0004_trans_0001",
    "unique_award_key": "award_procure_0004",
}


@fixture
def populate_initial_source_tables_pg():
    # Populate transactions.SourceAssistanceTransaction and associated broker.ExternalDataType data in Postgres
    for assist in initial_assists:
        baker.make("transactions.SourceAssistanceTransaction", **assist)

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_assistance_transaction",
        external_data_type_id=11,
        update_date=initial_source_table_load_datetime,
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date=initial_source_table_load_datetime, external_data_type=edt)

    # Populate transactions.SourceProcurementTransaction and associated broker.ExternalDataType data in Postgres
    for procure in initial_procures:
        baker.make("transactions.SourceProcurementTransaction", **procure)

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make(
        "broker.ExternalDataType",
        name="source_procurement_transaction",
        external_data_type_id=10,
        update_date=initial_source_table_load_datetime,
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date=initial_source_table_load_datetime, external_data_type=edt)

    # Also need to populate values for es_deletes, int.transaction_[fabs|fpds|normalized], int.awards,
    #   and id lookup tables in broker.ExternalData[Type|LoadDate] tables
    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt = baker.make("broker.ExternalDataType", name="es_deletes", external_data_type_id=102, update_date=None)
    baker.make("broker.ExternalDataLoadDate", last_load_date=BEGINNING_OF_TIME, external_data_type=edt)

    for table_name, id in zip(
        (
            "transaction_fpds",
            "transaction_fabs",
            "transaction_normalized",
            "awards",
            "transaction_id_lookup",
            "award_id_lookup",
        ),
        range(201, 207),
    ):
        edt = baker.make("broker.ExternalDataType", name=table_name, external_data_type_id=id, update_date=None)
        baker.make("broker.ExternalDataLoadDate", last_load_date=BEGINNING_OF_TIME, external_data_type=edt)


@dataclass
class TableLoadInfo:
    spark: SparkSession
    table_name: str
    data: Sequence[Dict[str, Any]]
    overwrite: Optional[bool] = False


def load_tables_to_delta(s3_data_bucket, load_source_tables=True, load_other_raw_tables=None):
    if load_source_tables:
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        load_delta_table_from_postgres("detached_award_procurement", s3_data_bucket)

    if load_other_raw_tables:
        for item in load_other_raw_tables:
            if isinstance(item, TableLoadInfo):
                load_dict_to_delta_table(item.spark, s3_data_bucket, "raw", item.table_name, item.data, item.overwrite)
            else:
                load_delta_table_from_postgres(item, s3_data_bucket)


class TestInitialRun:
    @staticmethod
    def initial_run(s3_data_bucket, load_source_tables=True, load_other_raw_tables=None, initial_copy=True):
        load_tables_to_delta(s3_data_bucket, load_source_tables, load_other_raw_tables)
        call_params = ["load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_data_bucket]
        if not initial_copy:
            call_params.append("--no-initial-copy")
        call_command(*call_params)

    @staticmethod
    def verify_transaction_ids(spark, expected_transaction_id_lookup, expected_last_load=None):
        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY transaction_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Verify max transaction id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_transaction_id = cursor.fetchone()[0]
        if expected_transaction_id_lookup:
            assert max_transaction_id == max(
                [transaction["transaction_id"] for transaction in expected_transaction_id_lookup]
            )
        else:
            assert max_transaction_id == 1

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_transaction_id}, false)")

        if isinstance(expected_last_load, datetime):
            assert get_last_load_date("transaction_id_lookup") == expected_last_load
        elif isinstance(expected_last_load, date):
            assert get_last_load_date("transaction_id_lookup").date() == expected_last_load

    @staticmethod
    def verify_award_ids(spark, expected_award_id_lookup, expected_last_load=None):
        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY award_id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        # Verify max award id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_award_id = cursor.fetchone()[0]
        if expected_award_id_lookup:
            assert max_award_id == max([award["award_id"] for award in expected_award_id_lookup])
        else:
            assert max_award_id == 1

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('award_id_seq', {max_award_id}, false)")

        if isinstance(expected_last_load, datetime):
            assert get_last_load_date("award_id_lookup") == expected_last_load
        elif isinstance(expected_last_load, date):
            assert get_last_load_date("award_id_lookup").date() == expected_last_load

    @staticmethod
    def verify_lookup_info(
        spark,
        expected_transaction_id_lookup,
        expected_award_id_lookup,
        expected_last_load_transaction_id_lookup=None,
        expected_load_load_award_id_lookup=None,
    ):
        TestInitialRun.verify_transaction_ids(
            spark, expected_transaction_id_lookup, expected_last_load_transaction_id_lookup
        )
        TestInitialRun.verify_award_ids(spark, expected_award_id_lookup, expected_load_load_award_id_lookup)

    @staticmethod
    def verify_raw_vs_int_tables(spark, table_name, col_names):
        # Make sure the raw and int versions of the given table match
        result = spark.sql(
            f"""
            SELECT {', '.join(col_names)} FROM int.{table_name}
            MINUS
            SELECT {', '.join(col_names)} FROM raw.{table_name}
            """
        ).collect()
        assert len(result) == 0

        result = spark.sql(
            f"""
            SELECT {', '.join(col_names)} FROM raw.{table_name}
            MINUS
            SELECT {', '.join(col_names)} FROM int.{table_name}
            """
        ).collect()
        assert len(result) == 0

    @staticmethod
    def verify(
        spark,
        expected_transaction_id_lookup,
        expected_award_id_lookup,
        expected_normalized_count=0,
        expected_fabs_count=0,
        expected_fpds_count=0,
        expected_last_load_transaction_id_lookup=None,
        expected_last_load_award_id_lookup=None,
        expected_last_load_transaction_normalized=None,
        expected_last_load_transaction_fabs=None,
        expected_last_load_transaction_fpds=None,
    ):
        TestInitialRun.verify_lookup_info(
            spark,
            expected_transaction_id_lookup,
            expected_award_id_lookup,
            expected_last_load_transaction_id_lookup,
            expected_last_load_award_id_lookup,
        )

        # int.award_ids_delete_modified should exist, but be empty
        actual_count = spark.sql("SELECT COUNT(*) AS count from int.award_ids_delete_modified").collect()[0]["count"]
        assert actual_count == 0

        # Make sure int.transaction_[normalized,fabs,fpds] tables have been created and have the expected sizes.
        for table_name, expected_count, expected_last_load, col_names in zip(
            (f"transaction_{t}" for t in ("normalized", "fabs", "fpds")),
            (expected_normalized_count, expected_fabs_count, expected_fpds_count),
            (
                expected_last_load_transaction_normalized,
                expected_last_load_transaction_fabs,
                expected_last_load_transaction_fpds,
            ),
            (list(TRANSACTION_NORMALIZED_COLUMNS), TRANSACTION_FABS_COLUMNS, TRANSACTION_FPDS_COLUMNS),
        ):
            actual_count = spark.sql(f"SELECT COUNT(*) AS count from int.{table_name}").collect()[0]["count"]
            assert actual_count == expected_count

            if isinstance(expected_last_load, datetime):
                assert get_last_load_date(table_name) == expected_last_load
            elif isinstance(expected_last_load, date):
                assert get_last_load_date(table_name).date() == expected_last_load

            if expected_count > 0:
                # Only verify raw vs int tables if raw table exists
                try:
                    spark.sql(f"SELECT 1 FROM raw.{table_name}")
                except pyspark.sql.utils.AnalysisException as e:
                    if re.match(rf"Table or view not found: raw\.{table_name}", e.desc):
                        pass
                    else:
                        raise e
                else:
                    TestInitialRun.verify_raw_vs_int_tables(spark, table_name, col_names)

    @mark.django_db(transaction=True)
    def test_edge_cases_using_only_source_tables(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # 1. Call initial run with no raw tables, except for published_fabs and detached_award_procurement.
        # Also, don't do initial copy of tables
        TestInitialRun.initial_run(s3_unittest_data_bucket, initial_copy=False)
        kwargs = {
            "expected_last_load_transaction_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_award_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(spark, [], [], **kwargs)

        # 2. Call initial run with existing, but empty raw.transaction_normalized and raw.awards tables

        # Make sure raw.transaction_normalized and raw.awards exist
        for table_name in ("transaction_normalized", "awards"):
            call_command(
                "create_delta_table", "--destination-table", table_name, "--spark-s3-bucket", s3_unittest_data_bucket
            )
        # Don't reload the source tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, False)
        TestInitialRun.verify(spark, [], [], **kwargs)


# Even though all the tests that use the Postgres loader have been removed, these variables are still
# needed for some tests.
class InitialRunWithPostgresLoader:
    expected_initial_transaction_id_lookup = [
        {
            "transaction_id": id,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[id - 1]["afa_generated_unique"].upper(),
        }
        for id in range(1, len(initial_assists) + 1)
    ] + [
        {
            "transaction_id": id,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[id - 6]["detached_award_proc_unique"].upper(),
        }
        for id in range(len(initial_assists) + 1, len(initial_assists) + len(initial_procures) + 1)
    ]

    expected_initial_award_id_lookup = [
        {
            "award_id": int(assist["unique_award_key"].split("_")[-1]),
            "is_fpds": False,
            "transaction_unique_id": assist["afa_generated_unique"].upper(),
            "generated_unique_award_id": assist["unique_award_key"].upper(),
        }
        for assist in initial_assists
    ] + [
        {
            "award_id": (
                int(procure["unique_award_key"].split("_")[-1])
                + max([int(assist["unique_award_key"].split("_")[-1]) for assist in initial_assists])
            ),
            "is_fpds": True,
            "transaction_unique_id": procure["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": procure["unique_award_key"].upper(),
        }
        for procure in initial_procures
    ]

    expected_initial_transaction_fabs = [
        {
            **assist,
            "action_date": dateutil.parser.parse(assist["action_date"]).date().isoformat(),
            "afa_generated_unique": assist["afa_generated_unique"].upper(),
            "transaction_id": assist["published_fabs_id"],
            "unique_award_key": assist["unique_award_key"].upper(),
        }
        for assist in initial_assists
    ]

    expected_initial_transaction_fpds = [
        {
            **procure,
            "action_date": procure["action_date"],
            "detached_award_proc_unique": procure["detached_award_proc_unique"].upper(),
            "transaction_id": procure["detached_award_procurement_id"] + len(initial_assists),
            "unique_award_key": procure["unique_award_key"].upper(),
        }
        for procure in initial_procures
    ]


class TestInitialRunNoPostgresLoader:
    expected_initial_transaction_id_lookup = [
        {
            "transaction_id": 1,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[0]["afa_generated_unique"].upper(),
        },
        {
            "transaction_id": 2,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[0]["detached_award_proc_unique"].upper(),
        },
        {
            "transaction_id": 3,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[1]["afa_generated_unique"].upper(),
        },
        {
            "transaction_id": 4,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[1]["detached_award_proc_unique"].upper(),
        },
        {
            "transaction_id": 5,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[2]["afa_generated_unique"].upper(),
        },
        {
            "transaction_id": 6,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[2]["detached_award_proc_unique"].upper(),
        },
        {
            "transaction_id": 7,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[3]["afa_generated_unique"].upper(),
        },
        {
            "transaction_id": 8,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[4]["afa_generated_unique"].upper(),
        },
        {
            "transaction_id": 9,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[3]["detached_award_proc_unique"].upper(),
        },
        {
            "transaction_id": 10,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[4]["detached_award_proc_unique"].upper(),
        },
    ]

    expected_initial_award_id_lookup = [
        {
            "award_id": 1,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[0]["afa_generated_unique"].upper(),
            "generated_unique_award_id": initial_assists[0]["unique_award_key"].upper(),
        },
        {
            "award_id": 2,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[1]["afa_generated_unique"].upper(),
            "generated_unique_award_id": initial_assists[1]["unique_award_key"].upper(),
        },
        {
            "award_id": 2,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[2]["afa_generated_unique"].upper(),
            "generated_unique_award_id": initial_assists[2]["unique_award_key"].upper(),
        },
        {
            "award_id": 3,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[0]["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": initial_procures[0]["unique_award_key"].upper(),
        },
        {
            "award_id": 4,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[1]["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": initial_procures[1]["unique_award_key"].upper(),
        },
        {
            "award_id": 4,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[2]["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": initial_procures[2]["unique_award_key"].upper(),
        },
        {
            "award_id": 5,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[3]["afa_generated_unique"].upper(),
            "generated_unique_award_id": initial_assists[3]["unique_award_key"].upper(),
        },
        {
            "award_id": 5,
            "is_fpds": False,
            "transaction_unique_id": initial_assists[4]["afa_generated_unique"].upper(),
            "generated_unique_award_id": initial_assists[4]["unique_award_key"].upper(),
        },
        {
            "award_id": 6,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[3]["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": initial_procures[3]["unique_award_key"].upper(),
        },
        {
            "award_id": 6,
            "is_fpds": True,
            "transaction_unique_id": initial_procures[4]["detached_award_proc_unique"].upper(),
            "generated_unique_award_id": initial_procures[4]["unique_award_key"].upper(),
        },
    ]

    initial_award_trans_norm_update_create_date = initial_datetime + timedelta(days=1)

    initial_awards = [
        {
            "id": 1,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_assists[0]["unique_award_key"].upper(),
            "is_fpds": False,
            "transaction_unique_id": initial_assists[0]["afa_generated_unique"].upper(),
            "subaward_count": 0,
        },
        {
            "id": 2,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_assists[1]["unique_award_key"].upper(),
            "is_fpds": False,
            "transaction_unique_id": initial_assists[1]["afa_generated_unique"].upper(),
            "subaward_count": 0,
        },
        {
            "id": 3,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_procures[0]["unique_award_key"].upper(),
            "is_fpds": True,
            "transaction_unique_id": initial_procures[0]["detached_award_proc_unique"].upper(),
            "subaward_count": 0,
        },
        {
            "id": 4,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_procures[1]["unique_award_key"].upper(),
            "is_fpds": True,
            "transaction_unique_id": initial_procures[1]["detached_award_proc_unique"].upper(),
            "subaward_count": 0,
        },
        {
            "id": 5,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_assists[3]["unique_award_key"].upper(),
            "is_fpds": False,
            "transaction_unique_id": initial_assists[3]["afa_generated_unique"].upper(),
            "subaward_count": 0,
        },
        {
            "id": 6,
            "update_date": initial_award_trans_norm_update_create_date,
            "generated_unique_award_id": initial_procures[3]["unique_award_key"].upper(),
            "is_fpds": True,
            "transaction_unique_id": initial_procures[3]["detached_award_proc_unique"].upper(),
            "subaward_count": 0,
        },
    ]

    initial_transaction_normalized = [
        {
            "id": 1,
            "award_id": 1,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_assists[0]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_assists[0]["afa_generated_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": False,
            "unique_award_key": initial_assists[0]["unique_award_key"].upper(),
        },
        {
            "id": 2,
            "award_id": 3,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_procures[0]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_procures[0]["detached_award_proc_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": True,
            "unique_award_key": initial_procures[0]["unique_award_key"].upper(),
        },
        {
            "id": 3,
            "award_id": 2,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_assists[1]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_assists[1]["afa_generated_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": False,
            "unique_award_key": initial_assists[1]["unique_award_key"].upper(),
        },
        {
            "id": 4,
            "award_id": 4,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_procures[1]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_procures[1]["detached_award_proc_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": True,
            "unique_award_key": initial_procures[1]["unique_award_key"].upper(),
        },
        {
            "id": 5,
            "award_id": 2,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_assists[2]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_assists[2]["afa_generated_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": False,
            "unique_award_key": initial_assists[2]["unique_award_key"].upper(),
        },
        {
            "id": 6,
            "award_id": 4,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_procures[2]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_procures[2]["detached_award_proc_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": True,
            "unique_award_key": initial_procures[2]["unique_award_key"].upper(),
        },
        {
            "id": 7,
            "award_id": 5,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_assists[3]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_assists[3]["afa_generated_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": False,
            "unique_award_key": initial_assists[3]["unique_award_key"].upper(),
        },
        {
            "id": 8,
            "award_id": 5,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_assists[4]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_assists[4]["afa_generated_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": False,
            "unique_award_key": initial_assists[4]["unique_award_key"].upper(),
        },
        {
            "id": 9,
            "award_id": 6,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_procures[3]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_procures[3]["detached_award_proc_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": True,
            "unique_award_key": initial_procures[3]["unique_award_key"].upper(),
        },
        {
            "id": 10,
            "award_id": 6,
            "business_categories": [],
            "action_date": dateutil.parser.parse(initial_procures[3]["action_date"]).date(),
            "create_date": initial_award_trans_norm_update_create_date,
            "transaction_unique_id": initial_procures[4]["detached_award_proc_unique"].upper(),
            "update_date": initial_award_trans_norm_update_create_date,
            "is_fpds": True,
            "unique_award_key": initial_procures[4]["unique_award_key"].upper(),
        },
    ]

    initial_transaction_fabs = [
        {
            **assist,
            "action_date": dateutil.parser.parse(assist["action_date"]).date().isoformat(),
            "afa_generated_unique": assist["afa_generated_unique"].upper(),
            "transaction_id": (assist["published_fabs_id"] - 1) * 2 + 1,
            "unique_award_key": assist["unique_award_key"].upper(),
        }
        for assist in initial_assists[:4]
    ] + [
        {
            **initial_assists[4],
            "action_date": dateutil.parser.parse(initial_assists[4]["action_date"]).date().isoformat(),
            "afa_generated_unique": initial_assists[4]["afa_generated_unique"].upper(),
            "transaction_id": 8,
            "unique_award_key": initial_assists[4]["unique_award_key"].upper(),
        }
    ]

    initial_transaction_fpds = [
        {
            **procure,
            "action_date": procure["action_date"],
            "detached_award_proc_unique": procure["detached_award_proc_unique"].upper(),
            "transaction_id": procure["detached_award_procurement_id"] * 2,
            "unique_award_key": procure["unique_award_key"].upper(),
        }
        for procure in initial_procures[:3]
    ] + [
        {
            **initial_procures[3],
            "action_date": initial_procures[3]["action_date"],
            "detached_award_proc_unique": initial_procures[3]["detached_award_proc_unique"].upper(),
            "transaction_id": 9,
            "unique_award_key": initial_procures[3]["unique_award_key"].upper(),
        },
        {
            **initial_procures[4],
            "action_date": initial_procures[4]["action_date"],
            "detached_award_proc_unique": initial_procures[4]["detached_award_proc_unique"].upper(),
            "transaction_id": 10,
            "unique_award_key": initial_procures[4]["unique_award_key"].upper(),
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
        load_dict_to_delta_table(
            spark, s3_unittest_data_bucket, "raw", "transaction_normalized", self.initial_transaction_normalized
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
            TableLoadInfo(spark, "awards", self.initial_awards),
        ]
        # Don't call Postgres loader, though.
        TestInitialRun.initial_run(
            s3_unittest_data_bucket, load_other_raw_tables=load_other_raw_tables, initial_copy=False
        )
        kwargs = {
            "expected_last_load_transaction_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_award_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(
            spark, self.expected_initial_transaction_id_lookup, self.expected_initial_award_id_lookup, **kwargs
        )

        # 2. Call initial_run with initial-copy, and have all raw tables populated

        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(spark, "transaction_fabs", self.initial_transaction_fabs),
            TableLoadInfo(spark, "transaction_fpds", self.initial_transaction_fpds),
        ]
        # Don't call Postgres loader or re-load the source tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, False, load_other_raw_tables)
        kwargs["expected_last_load_transaction_normalized"] = initial_source_table_load_datetime
        kwargs["expected_last_load_transaction_fabs"] = initial_source_table_load_datetime
        kwargs["expected_last_load_transaction_fpds"] = initial_source_table_load_datetime
        TestInitialRun.verify(
            spark,
            self.expected_initial_transaction_id_lookup,
            self.expected_initial_award_id_lookup,
            len(self.initial_transaction_normalized),
            len(self.initial_transaction_fabs),
            len(self.initial_transaction_fpds),
            **kwargs,
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
        TestInitialRun.initial_run(s3_unittest_data_bucket, initial_copy=False)
        kwargs = {
            "expected_last_load_transaction_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_award_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(spark, [], [], **kwargs)

        # Then, call load_transactions_in_delta with etl-level of transaction_id_lookup.
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # The expected transaction_id_lookup table should be the same as in InitialRunWithPostgresLoader,
        # but all of the transaction ids should be 1 larger than expected there.
        expected_transaction_id_lookup = deepcopy(InitialRunWithPostgresLoader.expected_initial_transaction_id_lookup)
        for item in expected_transaction_id_lookup:
            item["transaction_id"] += 1
        # Also, the last load date for the transaction_id_lookup table should be updated to the date of the
        # initial loads.
        kwargs["expected_last_load_transaction_id_lookup"] = initial_source_table_load_datetime
        TestInitialRun.verify(spark, expected_transaction_id_lookup, [], **kwargs)

    def happy_path_test_core(
        self,
        spark,
        s3_data_bucket,
        load_other_raw_tables,
        expected_initial_transaction_id_lookup,
        expected_initial_award_id_lookup,
        expected_transaction_id_lookup_pops,
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then transaction_id_lookup

        # Since these tests only care about the condition of the transaction_id_lookup table after various
        # operations, only load the essential tables to Delta, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(s3_data_bucket, load_other_raw_tables=load_other_raw_tables, initial_copy=False)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # With no deletes or inserts yet, the transaction_id_lookup table should be the same as after the initial run.
        # Also, the last load dates for the id lookup tables should match the load dates of the source tables.
        kwargs = {
            "expected_last_load_transaction_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_award_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(spark, expected_initial_transaction_id_lookup, expected_initial_award_id_lookup, **kwargs)

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
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY transaction_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup = deepcopy(expected_initial_transaction_id_lookup)
        expected_transaction_id_lookup.pop()
        expected_transaction_id_lookup.pop()
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Also, make sure transaction_id_seq hasn't gone backwards
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_transaction_id = cursor.fetchone()[0]
        assert max_transaction_id == (len(initial_assists) + len(initial_procures))

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value as previously.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_transaction_id}, false)")

        # 3. Test for a single inserted transaction, and another call to load_transaction_in_delta with etl-level of
        # transaction_id_lookup.

        # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
        # Postgres table, and then push the updated table to Delta.
        last_assist_load_datetime = datetime.now(timezone.utc)
        insert_datetime = last_assist_load_datetime + timedelta(minutes=-15)
        assist = deepcopy(new_assist)
        assist.update(
            {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
        )
        baker.make("transactions.SourceAssistanceTransaction", **assist)
        update_last_load_date("source_assistance_transaction", last_assist_load_datetime)
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY transaction_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_transaction_id_lookup.append(
            {
                "transaction_id": 11,
                "is_fpds": False,
                "transaction_unique_id": new_assist["afa_generated_unique"].upper(),
            }
        )
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        # Although the last load date for the source_assistance_transaction was updated above, the code in
        # load_transactions_in_delta takes the minimum last load date of that table and of the
        # source_procurement_transaction table, which has not been updated since the initial load of both tables.
        assert get_last_load_date("transaction_id_lookup") == initial_source_table_load_datetime

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # transaction_id_lookup, and test that the results are as expected.
        last_procure_load_datetime = datetime.now(timezone.utc)
        insert_datetime = last_procure_load_datetime + timedelta(minutes=-15)
        procure = deepcopy(new_procure)
        procure.update(
            {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
        )
        baker.make("transactions.SourceProcurementTransaction", **procure)
        update_last_load_date("source_procurement_transaction", last_procure_load_datetime)
        load_delta_table_from_postgres("detached_award_procurement", s3_data_bucket)

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
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY transaction_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        for pop in expected_transaction_id_lookup_pops:
            expected_transaction_id_lookup.pop(pop)
        expected_transaction_id_lookup.append(
            {
                "transaction_id": 12,
                "is_fpds": True,
                "transaction_unique_id": new_procure["detached_award_proc_unique"].upper(),
            }
        )
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")

        assert get_last_load_date("transaction_id_lookup") == last_assist_load_datetime

    @mark.django_db(transaction=True)
    def test_happy_path_scenarios_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(
                spark, "transaction_normalized", TestInitialRunNoPostgresLoader.initial_transaction_normalized
            ),
            TableLoadInfo(spark, "awards", TestInitialRunNoPostgresLoader.initial_awards),
        ]

        self.happy_path_test_core(
            spark,
            s3_unittest_data_bucket,
            load_other_raw_tables,
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup,
            (1, 1, 2),
        )


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
                "create_delta_table", "--destination-table", table_name, "--spark-s3-bucket", s3_unittest_data_bucket
            )

        # Then, call load_transactions_in_delta with etl-level of initial_run and verify.
        # Don't reload the source tables, and don't do initial copy of transaction tables, though.
        TestInitialRun.initial_run(s3_unittest_data_bucket, initial_copy=False)
        kwargs = {
            "expected_last_load_transaction_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_award_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(spark, [], [], **kwargs)

        # Then, call load_transactions_in_delta with etl-level of award_id_lookup.
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # The expected award_id_lookup table should be the same as in TestInitialRunWithPostgresLoader,
        # but all of the award ids should be 1 larger than expected there.
        expected_award_id_lookup = deepcopy(InitialRunWithPostgresLoader.expected_initial_award_id_lookup)
        for item in expected_award_id_lookup:
            item["award_id"] += 1
        # Also, the last load date for the award_id_lookup table should be updated to the date of the initial loads.
        kwargs["expected_last_load_award_id_lookup"] = initial_source_table_load_datetime
        TestInitialRun.verify(spark, [], expected_award_id_lookup, **kwargs)

    def happy_path_test_core(
        self,
        spark,
        s3_data_bucket,
        load_other_raw_tables,
        expected_initial_transaction_id_lookup,
        expected_initial_award_id_lookup,
        expected_award_id_lookup_pops,
        partially_deleted_award_id,
    ):
        # 1. Test calling load_transactions_in_delta with the etl-level set to the proper sequencing of
        # initial_run, then award_id_lookup

        # Since these tests only care about the condition of the award_id_lookup table after various
        # operations, only load the essential tables to Delta, and don't copy the raw transaction tables to int.
        TestInitialRun.initial_run(s3_data_bucket, load_other_raw_tables=load_other_raw_tables, initial_copy=False)
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # With no deletes or inserts, the award_id_lookup table should be the same as after the initial run.
        # Also, the last load dates for the id lookup tables should match the load dates of the source tables.
        kwargs = {
            "expected_last_load_transaction_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_award_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        TestInitialRun.verify(spark, expected_initial_transaction_id_lookup, expected_initial_award_id_lookup, **kwargs)

        # 2. Test deleting the transactions with the last award ID from the appropriate raw table,
        # followed by a call to load_transaction_in_delta with etl-level of award_id_lookup
        spark.sql(
            """
            DELETE FROM raw.detached_award_procurement
            WHERE detached_award_procurement_id = 4 OR detached_award_procurement_id = 5
        """
        )
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY award_id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup = deepcopy(expected_initial_award_id_lookup)
        expected_award_id_lookup.pop()
        expected_award_id_lookup.pop()
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        # Make sure award_id_seq hasn't gone backwards
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            max_award_id = cursor.fetchone()[0]
        assert max_award_id == max([award["id"] for award in TestInitialRunNoPostgresLoader.initial_awards])

        # Since this test just called nextval(), need to reset the sequence with the is_called flag set to false
        # so that the next call to nextval() will return the same value as previously.
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('award_id_seq', {max_award_id}, false)")

        # Also, test that int.award_ids_delete_modified is still empty, since all transactions associated with the
        # final award were deleted.
        actual_count = spark.sql(f"SELECT COUNT(*) AS count from int.award_ids_delete_modified").collect()[0]["count"]
        assert actual_count == 0

        # 3. Test for a single inserted transaction, and another call to load_transaction_in_delta with etl-level of
        # award_id_lookup.

        # Can't use spark.sql to just insert rows with only values for desired columns (need to specify values for
        # all of them), so using model baker to add new rows to Postgres table, and then pushing new table to Delta.
        last_assist_load_datetime = datetime.now(timezone.utc)
        insert_datetime = last_assist_load_datetime + timedelta(minutes=-15)
        assist = deepcopy(new_assist)
        assist.update(
            {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
        )
        baker.make("transactions.SourceAssistanceTransaction", **assist)
        update_last_load_date("source_assistance_transaction", last_assist_load_datetime)
        load_delta_table_from_postgres("published_fabs", s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY award_id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        expected_award_id_lookup.append(
            {
                "award_id": 7,
                "is_fpds": False,
                "transaction_unique_id": new_assist["afa_generated_unique"].upper(),
                "generated_unique_award_id": new_assist["unique_award_key"].upper(),
            }
        )
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        # Although the last load date for the source_assistance_transaction was updated above, the code in
        # load_transactions_in_delta takes the minimum last load date of that table and of the
        # source_procurement_transaction table, which has not been updated since the initial load of both tables.
        assert get_last_load_date("award_id_lookup") == initial_source_table_load_datetime

        # Also, test that int.award_ids_delete_modified is still empty, since no transactions were deleted since
        # the last check.
        actual_count = spark.sql("SELECT COUNT(*) AS count from int.award_ids_delete_modified").collect()[0]["count"]
        assert actual_count == 0

        # 4. Make inserts to and deletes from the raw tables, call load_transaction_in_delta with etl-level of
        # award_id_lookup, and test that the results are as expected, and that int.award_ids_delete_modified has
        # tracked the appropriate delete.
        last_procure_load_datetime = datetime.now(timezone.utc)
        insert_datetime = last_procure_load_datetime + timedelta(minutes=-15)
        procure = deepcopy(new_procure)
        procure.update(
            {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
        )
        baker.make("transactions.SourceProcurementTransaction", **procure)
        update_last_load_date("source_procurement_transaction", last_procure_load_datetime)
        load_delta_table_from_postgres("detached_award_procurement", s3_data_bucket)

        spark.sql(
            """
            DELETE FROM raw.published_fabs
            WHERE published_fabs_id = 2
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
        query = "SELECT * FROM int.award_id_lookup ORDER BY award_id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]

        for pop in expected_award_id_lookup_pops:
            expected_award_id_lookup.pop(pop)
        expected_award_id_lookup.append(
            {
                "award_id": 8,
                "is_fpds": True,
                "transaction_unique_id": new_procure["detached_award_proc_unique"].upper(),
                "generated_unique_award_id": new_procure["unique_award_key"].upper(),
            }
        )
        assert equal_datasets(expected_award_id_lookup, delta_data, "")

        assert get_last_load_date("award_id_lookup") == last_assist_load_datetime

        # Verify award_ids_delete_modified table
        query = "SELECT * FROM int.award_ids_delete_modified ORDER BY award_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets([{"award_id": partially_deleted_award_id}], delta_data, "")

    @mark.django_db(transaction=True)
    def test_happy_path_scenarios_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        # Since we're not using the Postgres transaction loader, load raw.transaction_normalized and raw.awards
        # from expected data when making initial run
        load_other_raw_tables = [
            TableLoadInfo(
                spark, "transaction_normalized", TestInitialRunNoPostgresLoader.initial_transaction_normalized
            ),
            TableLoadInfo(spark, "awards", TestInitialRunNoPostgresLoader.initial_awards),
        ]

        self.happy_path_test_core(
            spark,
            s3_unittest_data_bucket,
            load_other_raw_tables,
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup,
            (3, 1),
            2,
        )


class TransactionFabsFpdsCore:

    new_transaction_fabs_fpds_id = 6
    new_transaction_id = 11

    def __init__(
        self,
        spark,
        s3_data_bucket,
        etl_level,
        pk_field,
        compare_fields,
        usas_source_table_name,
        broker_source_table_name,
        baker_table,
        baker_kwargs,
        expected_initial_transaction_fabs,
        expected_initial_transaction_fpds,
    ):
        self.spark = spark
        self.s3_data_bucket = s3_data_bucket
        self.etl_level = etl_level
        self.pk_field = pk_field
        self.usas_source_table_name = usas_source_table_name
        self.broker_source_table_name = broker_source_table_name
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
        TestInitialRun.initial_run(self.s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the transaction and award id lookup tables and other int transaction tables.  They should all be empty.
        kwargs = {
            "expected_last_load_transaction_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_award_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        # Even though nothing will have been loaded to that table, the table whose etl_level has been called will
        # have its last load date set to the date of the source tables' load.
        kwargs[f"expected_last_load_{self.etl_level}"] = initial_source_table_load_datetime
        TestInitialRun.verify(self.spark, [], [], **kwargs)

        # 3. With raw.transaction_normalized and raw.awards still not created, call load_transactions_in_delta
        # with etl-level of transaction_id_lookup, and then again with etl-level of transaction_f[ab|pd]s.

        # Since the call to load_transactions_in_delta with etl-level of transaction_f[ab|pd]s above succeeded, we first
        # need to reset the last load date on transaction_fabs
        update_last_load_date(self.etl_level, BEGINNING_OF_TIME)

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # The expected transaction_id_lookup table should be the same as in InitialRunWithPostgresLoader,
        # but all of the transaction ids should be 1 larger than expected there.
        expected_transaction_id_lookup = deepcopy(InitialRunWithPostgresLoader.expected_initial_transaction_id_lookup)
        for item in expected_transaction_id_lookup:
            item["transaction_id"] += 1
        # Also, the last load date of the transaction_id_lookup table and of the table whose etl_level is being
        # called should be updated to the load time of the source tables
        kwargs["expected_last_load_transaction_id_lookup"] = initial_source_table_load_datetime
        kwargs[f"expected_last_load_{self.etl_level}"] = initial_source_table_load_datetime
        TestInitialRun.verify(
            self.spark,
            expected_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            **kwargs,
        )

        # Verify key fields in transaction_f[ab|pd]s table.  Note that the transaction_ids should be 1 more than
        # in those from InitialRunWithPostgresLoader
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
        self,
        load_other_raw_tables,
        expected_initial_transaction_id_lookup,
    ):
        # 1. Call load_transactions_in_delta with etl-level of initial_run first, making sure to load
        # raw.transaction_normalized along with the source tables, but don't copy the raw tables to int.
        # Then immediately call load_transactions_in_delta with etl-level of transaction_f[ab|pd]s.
        TestInitialRun.initial_run(self.s3_data_bucket, load_other_raw_tables=load_other_raw_tables, initial_copy=False)
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Even without the call to load_transactions_in_delta with etl-level of transaction_id_lookup, the appropriate
        # data will be populated in the transaction_id_lookup table via initial_run to allow the call to
        # load_transactions_in_delta with etl-level of transaction_fabs to populate int.transaction_fabs correctly with
        # the initial data.
        kwargs = {
            "expected_last_load_transaction_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_award_id_lookup": BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        kwargs[f"expected_last_load_{self.etl_level}"] = initial_source_table_load_datetime
        TestInitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            **kwargs,
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
        last_load_datetime = datetime.now(timezone.utc)
        insert_update_datetime = last_load_datetime + timedelta(minutes=-15)
        self.baker_kwargs.update(
            {
                "action_date": insert_update_datetime.isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime,
            }
        )
        baker.make(self.baker_table, **self.baker_kwargs)
        update_last_load_date(self.broker_source_table_name, last_load_datetime)
        load_delta_table_from_postgres(self.usas_source_table_name, self.s3_data_bucket)

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET updated_at = '{insert_update_datetime}'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                DELETE FROM raw.{self.usas_source_table_name}
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
            **kwargs,
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
    def unexpected_paths_no_pg_loader_test(transaction_fabs_fpds_core):
        transaction_fabs_fpds_core.unexpected_paths_test_core(
            [
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    "transaction_normalized",
                    TestInitialRunNoPostgresLoader.initial_transaction_normalized,
                )
            ],
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
        )

    def happy_paths_test_core(
        self,
        load_other_raw_tables,
        expected_initial_transaction_id_lookup,
        expected_initial_award_id_lookup,
        expected_transaction_id_lookup_pops,
        expected_transaction_id_lookup_append,
        expected_transaction_fabs_fpds_append,
    ):
        # 1, Test calling load_transactions_in_delta with etl-level of transaction_f[ab|pd]s after calling with
        # etl-levels of initial_run and transaction_id_lookup.
        TestInitialRun.initial_run(self.s3_data_bucket, load_other_raw_tables=load_other_raw_tables)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the tables.  The transaction and award id lookup tables should be the same as during the initial run.
        # The transaction_normalized and transaction_f[ab|pd]s tables should have been copied from raw to int.
        kwargs = {
            "expected_last_load_transaction_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_award_id_lookup": initial_source_table_load_datetime,
            "expected_last_load_transaction_normalized": initial_source_table_load_datetime,
            "expected_last_load_transaction_fabs": BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": BEGINNING_OF_TIME,
        }
        kwargs[f"expected_last_load_{self.etl_level}"] = initial_source_table_load_datetime
        TestInitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            expected_initial_award_id_lookup,
            len(expected_initial_transaction_id_lookup),
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            **kwargs,
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
        last_load_datetime = datetime.now(timezone.utc)
        insert_update_datetime = last_load_datetime + timedelta(minutes=-15)
        self.baker_kwargs.update(
            {
                "action_date": insert_update_datetime.isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime,
            }
        )
        baker.make(self.baker_table, **self.baker_kwargs)
        update_last_load_date(self.broker_source_table_name, last_load_datetime)
        load_delta_table_from_postgres(self.usas_source_table_name, self.s3_data_bucket)

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET updated_at = '{insert_update_datetime}'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                DELETE FROM raw.{self.usas_source_table_name}
                WHERE {self.pk_field} = 2 OR {self.pk_field} = 3
            """
        )

        # Need to load changes into the transaction_id_lookup table.
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY transaction_id"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]

        expected_transaction_id_lookup = deepcopy(expected_initial_transaction_id_lookup)
        for pop_index in expected_transaction_id_lookup_pops:
            expected_transaction_id_lookup.pop(pop_index)
        expected_transaction_id_lookup_append.update(
            {
                "transaction_id": self.new_transaction_id,
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
                "action_date": insert_update_datetime.date().isoformat()
                if self.etl_level == "transaction_fabs"
                else insert_update_datetime.isoformat(),
                "created_at": insert_update_datetime,
                "updated_at": insert_update_datetime,
            }
        )
        expected_transaction_fabs_fpds.append(expected_transaction_fabs_fpds_append)
        assert equal_datasets(expected_transaction_fabs_fpds, delta_data, "")

        # Verify that the last_load_dates of the transaction_id_lookup table and the table whose etl_level has been
        # called did NOT change, since only one of the broker source tables' last load date was changed.
        assert get_last_load_date("transaction_id_lookup") == initial_source_table_load_datetime
        assert get_last_load_date(self.etl_level) == initial_source_table_load_datetime

    @staticmethod
    def happy_paths_no_pg_loader_test(
        transaction_fabs_fpds_core,
        initial_transaction_fabs_fpds,
        expected_transaction_id_lookup_pops,
        expected_transaction_id_lookup_append,
        expected_transaction_fabs_fpds_append,
    ):
        transaction_fabs_fpds_core.happy_paths_test_core(
            (
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    "transaction_normalized",
                    TestInitialRunNoPostgresLoader.initial_transaction_normalized,
                ),
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark,
                    transaction_fabs_fpds_core.etl_level,
                    initial_transaction_fabs_fpds,
                ),
                TableLoadInfo(
                    transaction_fabs_fpds_core.spark, "awards", TestInitialRunNoPostgresLoader.initial_awards
                ),
            ),
            TestInitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            TestInitialRunNoPostgresLoader.expected_initial_award_id_lookup,
            expected_transaction_id_lookup_pops,
            expected_transaction_id_lookup_append,
            expected_transaction_fabs_fpds_append,
        )


class TestTransactionFabs:

    etl_level = "transaction_fabs"
    pk_field = "published_fabs_id"
    usas_source_table_name = "published_fabs"
    broker_source_table_name = "source_assistance_transaction"
    baker_table = "transactions.SourceAssistanceTransaction"
    compare_fields = InitialRunWithPostgresLoader.expected_initial_transaction_fabs[0].keys()
    new_afa_generated_unique = "award_assist_0004_trans_0001"
    new_unique_award_key = "award_assist_0004"
    baker_kwargs = {
        "published_fabs_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "afa_generated_unique": new_afa_generated_unique,
        "is_active": True,
        "unique_award_key": new_unique_award_key,
    }
    expected_transaction_id_lookup_append = {
        "is_fpds": False,
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
            self.usas_source_table_name,
            self.broker_source_table_name,
            self.baker_table,
            deepcopy(self.baker_kwargs),
            expected_initial_transaction_fabs,
            [],
        )

    @mark.django_db(transaction=True)
    def test_unexpected_paths_source_tables_only(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self.generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, InitialRunWithPostgresLoader.expected_initial_transaction_fabs
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.unexpected_paths_no_pg_loader_test(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fabs
            )
        )

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.happy_paths_no_pg_loader_test(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fabs
            ),
            TestInitialRunNoPostgresLoader.initial_transaction_fabs,
            (2, 3),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append,
        )


class TestTransactionFpds:

    etl_level = "transaction_fpds"
    pk_field = "detached_award_procurement_id"
    usas_source_table_name = "detached_award_procurement"
    broker_source_table_name = "source_procurement_transaction"
    baker_table = "transactions.SourceProcurementTransaction"
    compare_fields = InitialRunWithPostgresLoader.expected_initial_transaction_fpds[0].keys()
    new_detached_award_proc_unique = "award_procure_0004_trans_0001"
    new_unique_award_key = "award_procure_0004"
    baker_kwargs = {
        "detached_award_procurement_id": TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "detached_award_proc_unique": new_detached_award_proc_unique,
        "unique_award_key": new_unique_award_key,
    }
    expected_transaction_id_lookup_append = {
        "is_fpds": True,
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
            self.usas_source_table_name,
            self.broker_source_table_name,
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
            spark, s3_unittest_data_bucket, InitialRunWithPostgresLoader.expected_initial_transaction_fpds
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.unexpected_paths_no_pg_loader_test(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fpds
            )
        )

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_source_tables_pg
    ):
        TransactionFabsFpdsCore.happy_paths_no_pg_loader_test(
            self.generate_transaction_fabs_fpds_core(
                spark, s3_unittest_data_bucket, TestInitialRunNoPostgresLoader.initial_transaction_fpds
            ),
            TestInitialRunNoPostgresLoader.initial_transaction_fpds,
            (3, 4),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append,
        )
