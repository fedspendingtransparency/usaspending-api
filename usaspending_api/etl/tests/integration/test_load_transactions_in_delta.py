"""Automated Unit Tests for the the loading of transaction and award tables in Delta Lake.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from copy import deepcopy
from datetime import date, datetime, timezone

import pyspark

from model_bakery import baker
from pytest import fixture, mark, raises

from django.db import connection
from django.core.management import call_command

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.etl.tests.integration.test_load_to_from_delta import load_delta_table_from_postgres, equal_datasets


@fixture
def populate_initial_postgres_data():
    # Populate transactions.SourceAssistanceTransaction and associated broker.ExternalDataType data
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=1,
        afa_generated_unique="award_assist_0001_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0001",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=2,
        afa_generated_unique="award_assist_0002_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=3,
        afa_generated_unique="award_assist_0002_trans_0002",
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=4,
        afa_generated_unique="award_assist_0003_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        is_active=True,
        unique_award_key="award_assist_0003",
    )
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=5,
        afa_generated_unique="award_assist_0003_trans_0002",
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

    # Populate transactions.SourceProcurementTransaction and associated broker.ExternalDataType data
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=1,
        detached_award_proc_unique="award_procure_0001_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0001",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=2,
        detached_award_proc_unique="award_procure_0002_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=3,
        detached_award_proc_unique="award_procure_0002_trans_0002",
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=4,
        detached_award_proc_unique="award_procure_0003_trans_0001",
        updated_at=datetime(year=2022, month=10, day=31),
        unique_award_key="award_procure_0003",
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id=5,
        detached_award_proc_unique="award_procure_0003_trans_0002",
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

    # Need to create awards before creating transactions
    assist_awards = []
    procure_awards = []

    assist_awards.append(
        baker.make(
            "awards.Award",
            id=1,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_assist_0001",
            is_fpds=False,
        )
    )
    assist_awards.append(
        baker.make(
            "awards.Award",
            id=2,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_assist_0002",
            is_fpds=False,
        )
    )
    procure_awards.append(
        baker.make(
            "awards.Award",
            id=3,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_procure_0001",
            is_fpds=True,
        )
    )
    procure_awards.append(
        baker.make(
            "awards.Award",
            id=4,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_procure_0002",
            is_fpds=True,
        )
    )
    assist_awards.append(
        baker.make(
            "awards.Award",
            id=5,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_assist_0003",
            is_fpds=False,
        )
    )
    procure_awards.append(
        baker.make(
            "awards.Award",
            id=6,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_procure_0003",
            is_fpds=True,
        )
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt_awards = baker.make(
        "broker.ExternalDataType", name="awards", external_data_type_id=204, update_date="2022-10-31"
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-10-31", external_data_type=edt_awards)

    # Create transactions in transaction_normalized
    baker.make(
        "awards.TransactionNormalized",
        id=1,
        award=assist_awards[0],
        transaction_unique_id="award_assist_0001_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=False,
        unique_award_key="award_assist_0001",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=2,
        award=procure_awards[0],
        transaction_unique_id="award_procure_0001_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=True,
        unique_award_key="award_procure_0001",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=3,
        award=assist_awards[1],
        transaction_unique_id="award_assist_0002_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=False,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=4,
        award=procure_awards[1],
        transaction_unique_id="award_procure_0002_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=True,
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=5,
        award=assist_awards[1],
        transaction_unique_id="award_assist_0002_trans_0002",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=False,
        unique_award_key="award_assist_0002",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=6,
        award=procure_awards[1],
        transaction_unique_id="award_procure_0002_trans_0002",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=True,
        unique_award_key="award_procure_0002",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=7,
        award=assist_awards[2],
        transaction_unique_id="award_assist_0003_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=False,
        unique_award_key="award_assist_0003",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=8,
        award=assist_awards[2],
        transaction_unique_id="award_assist_0003_trans_0002",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=False,
        unique_award_key="award_assist_0003",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=9,
        award=procure_awards[2],
        transaction_unique_id="award_procure_0003_trans_0001",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=True,
        unique_award_key="award_procure_0003",
    )
    baker.make(
        "awards.TransactionNormalized",
        id=10,
        award=procure_awards[2],
        transaction_unique_id="award_procure_0003_trans_0002",
        update_date=datetime(year=2022, month=10, day=31),
        is_fpds=True,
        unique_award_key="award_procure_0003",
    )

    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt_tn = baker.make(
        "broker.ExternalDataType", name="transaction_normalized", external_data_type_id=203, update_date="2022-10-31"
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="2022-10-31", external_data_type=edt_tn)

    # Need to populate values for transaction_id_lookup and award_id_lookup in broker.ExternalData[Type|LoadDate] tables
    # `name` and `external_data_type_id` must match those in `usaspending.broker.lookups`
    edt_tidlu = baker.make(
        "broker.ExternalDataType", name="transaction_id_lookup", external_data_type_id=205, update_date=None
    )
    edt_aidlu = baker.make(
        "broker.ExternalDataType", name="award_id_lookup", external_data_type_id=206, update_date=None
    )
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_tidlu)
    baker.make("broker.ExternalDataLoadDate", last_load_date="1970-01-01", external_data_type=edt_aidlu)


def load_initial_delta_tables(spark, s3_data_bucket):
    # Load tables and ensure they have loaded correctly
    load_delta_table_from_postgres("published_fabs", s3_data_bucket)
    load_delta_table_from_postgres("detached_award_procurement", s3_data_bucket)
    load_delta_table_from_postgres("transaction_normalized", s3_data_bucket)
    load_delta_table_from_postgres("awards", s3_data_bucket)

    # Also, make sure int database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS int")


class TestInitialRun:
    expected_transaction_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001",
        },
        {
            "id": 2,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001",
        },
        {
            "id": 3,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001",
        },
        {
            "id": 4,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001",
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002",
        },
        {
            "id": 6,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002",
        },
        {
            "id": 7,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001",
        },
        {
            "id": 8,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002",
        },
        {
            "id": 9,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001",
        },
        {
            "id": 10,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002",
        },
    ]

    expected_award_id_lookup = [
        {
            "id": 1,
            "detached_award_procurement_id": None,
            "published_fabs_id": 1,
            "transaction_unique_id": "award_assist_0001_trans_0001",
            "generated_unique_award_id": "award_assist_0001",
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 2,
            "transaction_unique_id": "award_assist_0002_trans_0001",
            "generated_unique_award_id": "award_assist_0002",
        },
        {
            "id": 2,
            "detached_award_procurement_id": None,
            "published_fabs_id": 3,
            "transaction_unique_id": "award_assist_0002_trans_0002",
            "generated_unique_award_id": "award_assist_0002",
        },
        {
            "id": 3,
            "detached_award_procurement_id": 1,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0001_trans_0001",
            "generated_unique_award_id": "award_procure_0001",
        },
        {
            "id": 4,
            "detached_award_procurement_id": 2,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0001",
            "generated_unique_award_id": "award_procure_0002",
        },
        {
            "id": 4,
            "detached_award_procurement_id": 3,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0002_trans_0002",
            "generated_unique_award_id": "award_procure_0002",
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 4,
            "transaction_unique_id": "award_assist_0003_trans_0001",
            "generated_unique_award_id": "award_assist_0003",
        },
        {
            "id": 5,
            "detached_award_procurement_id": None,
            "published_fabs_id": 5,
            "transaction_unique_id": "award_assist_0003_trans_0002",
            "generated_unique_award_id": "award_assist_0003",
        },
        {
            "id": 6,
            "detached_award_procurement_id": 4,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0001",
            "generated_unique_award_id": "award_procure_0003",
        },
        {
            "id": 6,
            "detached_award_procurement_id": 5,
            "published_fabs_id": None,
            "transaction_unique_id": "award_procure_0003_trans_0002",
            "generated_unique_award_id": "award_procure_0003",
        },
    ]

    # The unique_award_key field in transaction_normalized allows for NULLs in both Postgres and Delta,
    # so test for NULLs originating from both sources.
    @mark.django_db(transaction=True)
    def test_one_null_in_trans_norm_unique_award_key_from_pg(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            updated_at=datetime(year=2022, month=10, day=31),
            unique_award_key="award_procure_0004",
        )
        award = baker.make(
            "awards.Award",
            id=7,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_procure_0004",
            is_fpds=True,
        )
        baker.make(
            "awards.TransactionNormalized",
            id=11,
            award=award,
            transaction_unique_id="award_procure_0004_trans_0001",
            update_date=datetime(year=2022, month=10, day=31),
            is_fpds=True,
            unique_award_key=None,
        )

        load_initial_delta_tables(spark, s3_unittest_data_bucket)

        with raises(ValueError, match="Found 1 NULL in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

    @mark.django_db(transaction=True)
    def test_multiple_nulls_in_trans_norm_unique_award_key_from_pg(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            updated_at=datetime(year=2022, month=10, day=31),
            unique_award_key="award_procure_0004",
        )
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=7,
            detached_award_proc_unique="award_procure_0004_trans_0002",
            updated_at=datetime(year=2022, month=10, day=31),
            unique_award_key="award_procure_0004",
        )
        award = baker.make(
            "awards.Award",
            id=7,
            update_date=datetime(year=2022, month=10, day=31),
            generated_unique_award_id="award_procure_0004",
            is_fpds=True,
        )
        baker.make(
            "awards.TransactionNormalized",
            id=11,
            award=award,
            transaction_unique_id="award_procure_0004_trans_0001",
            update_date=datetime(year=2022, month=10, day=31),
            is_fpds=True,
            unique_award_key=None,
        )
        baker.make(
            "awards.TransactionNormalized",
            id=12,
            award=award,
            transaction_unique_id="award_procure_0004_trans_0002",
            update_date=datetime(year=2022, month=10, day=31),
            is_fpds=True,
            unique_award_key=None,
        )

        load_initial_delta_tables(spark, s3_unittest_data_bucket)

        with raises(ValueError, match="Found 2 NULLs in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

    @mark.django_db(transaction=True)
    def test_multiple_nulls_in_trans_norm_unique_award_key_from_delta(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        load_initial_delta_tables(spark, s3_unittest_data_bucket)

        spark.sql(
            """
            UPDATE raw.transaction_normalized
            SET unique_award_key = NULL
            WHERE id = 5 OR id = 6
        """
        )

        with raises(ValueError, match="Found 2 NULLs in 'unique_award_key' in table raw.transaction_normalized!"):
            call_command(
                "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
            )

    @staticmethod
    def initial_run(spark, s3_data_bucket):
        load_initial_delta_tables(spark, s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_data_bucket)

    @staticmethod
    def happy_verify_transaction_ids(spark):
        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(TestInitialRun.expected_transaction_id_lookup, delta_data, "")

        # Verify max transaction id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            max_transaction_id = cursor.fetchone()[0] - 1
        assert max_transaction_id == 10

        # Verify last load date (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("transaction_id_lookup")
        assert date(last_load_dt.year, last_load_dt.month, last_load_dt.day) == date.today()

    @staticmethod
    def happy_verify_award_ids(spark):
        # Verify award_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(TestInitialRun.expected_award_id_lookup, delta_data, "")

        # Verify max award id
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            max_award_id = cursor.fetchone()[0] - 1
        assert max_award_id == 6

        # Verify last load date (NOTE: get_last_load_date actually returns a datetime)
        # Ignoring the possibility that the test starts on one day, but reaches this code the next day.
        last_load_dt = get_last_load_date("award_id_lookup")
        assert date(last_load_dt.year, last_load_dt.month, last_load_dt.day) == date.today()

    @staticmethod
    def happy_verify(spark):
        TestInitialRun.happy_verify_transaction_ids(spark)
        TestInitialRun.happy_verify_award_ids(spark)

    @mark.django_db(transaction=True)
    def test_happy(self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data):
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)
        TestInitialRun.happy_verify(spark)

    @mark.django_db(transaction=True)
    def test_run_twice(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        # Verify that calling initial_run twice yields the same results as calling it once.
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)
        call_command(
            "load_transactions_in_delta", "--etl-level", "initial_run", "--spark-s3-bucket", s3_unittest_data_bucket
        )
        TestInitialRun.happy_verify(spark)


class TestTransactionIdLookup:
    @mark.django_db(transaction=True)
    def test_no_initial_run(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        load_initial_delta_tables(spark, s3_unittest_data_bucket)

        with raises(pyspark.sql.utils.AnalysisException, match="Table or view not found: int.transaction_id_lookup"):
            call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

    @mark.django_db(transaction=True)
    def test_no_deletes_or_inserts(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        # With no deletes or inserts, the transaction_lookup_id table should be the same as after
        # the initial run
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        TestInitialRun.happy_verify_transaction_ids(spark)

    @mark.django_db(transaction=True)
    def test_inserts_and_deletes(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)

        # Can't use spark.sql to just insert rows with only values for desired columns (need to specify values for
        # all of them), so using model baker to add new rows to Postgres table, and then pushing new table to Delta.
        insert_time = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=6,
            afa_generated_unique="award_assist_0004_trans_0001",
            updated_at=insert_time,
            is_active=True,
            unique_award_key="award_assist_0004",
        )
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=7,
            afa_generated_unique="award_assist_0005_trans_0001",
            updated_at=insert_time,
            is_active=False,
            unique_award_key="award_assist_0005",
        )
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            updated_at=insert_time,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)
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
                OR detached_award_procurement_id = 4 OR detached_award_procurement_id = 5
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")

        expected_transaction_id_lookup = deepcopy(TestInitialRun.expected_transaction_id_lookup)
        expected_transaction_id_lookup.pop()
        expected_transaction_id_lookup.pop()
        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(1)
        expected_transaction_id_lookup.pop(2)
        expected_transaction_id_lookup.extend(
            [
                {
                    "id": 11,
                    "detached_award_procurement_id": None,
                    "published_fabs_id": 6,
                    "transaction_unique_id": "award_assist_0004_trans_0001",
                },
                {
                    "id": 12,
                    "detached_award_procurement_id": 6,
                    "published_fabs_id": None,
                    "transaction_unique_id": "award_procure_0004_trans_0001",
                },
            ]
        )

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.transaction_id_lookup ORDER BY id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_transaction_id_lookup, delta_data, "")


class TestAwardIdLookup:
    @mark.django_db(transaction=True)
    def test_no_initial_run(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        load_initial_delta_tables(spark, s3_unittest_data_bucket)

        with raises(pyspark.sql.utils.AnalysisException, match="Table or view not found: int.award_id_lookup"):
            call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

    @mark.django_db(transaction=True)
    def test_no_deletes_or_inserts(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        # With no deletes or inserts, the award_lookup_id table should be the same as after
        # the initial run
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")
        TestInitialRun.happy_verify_award_ids(spark)

    @mark.django_db(transaction=True)
    def test_inserts_and_deletes(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, populate_initial_postgres_data
    ):
        TestInitialRun.initial_run(spark, s3_unittest_data_bucket)

        # Can't use spark.sql to just insert rows with only values for desired columns (need to specify values for
        # all of them), so using model baker to add new rows to Postgres table, and then pushing new table to Delta.
        insert_time = datetime.now(timezone.utc)
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=6,
            afa_generated_unique="award_assist_0004_trans_0001",
            updated_at=insert_time,
            is_active=False,
            unique_award_key="award_assist_0004",
        )
        baker.make(
            "transactions.SourceAssistanceTransaction",
            published_fabs_id=7,
            afa_generated_unique="award_assist_0005_trans_0001",
            updated_at=insert_time,
            is_active=True,
            unique_award_key="award_assist_0005",
        )
        baker.make(
            "transactions.SourceProcurementTransaction",
            detached_award_procurement_id=6,
            detached_award_proc_unique="award_procure_0004_trans_0001",
            updated_at=insert_time,
            unique_award_key="award_procure_0004",
        )
        load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)
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
            WHERE detached_award_procurement_id = 1 OR detached_award_procurement_id = 4
                OR detached_award_procurement_id = 5
        """
        )

        call_command("load_transactions_in_delta", "--etl-level", "award_id_lookup")

        expected_award_id_lookup = deepcopy(TestInitialRun.expected_award_id_lookup)
        expected_award_id_lookup.pop()
        expected_award_id_lookup.pop()
        expected_award_id_lookup.pop(3)
        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.pop(1)
        expected_award_id_lookup.extend(
            [
                {
                    "id": 7,
                    "detached_award_procurement_id": None,
                    "published_fabs_id": 7,
                    "transaction_unique_id": "award_assist_0005_trans_0001",
                    "generated_unique_award_id": "award_assist_0005",
                },
                {
                    "id": 8,
                    "detached_award_procurement_id": 6,
                    "published_fabs_id": None,
                    "transaction_unique_id": "award_procure_0004_trans_0001",
                    "generated_unique_award_id": "award_procure_0004",
                },
            ]
        )

        # Verify transaction_id_lookup table
        query = "SELECT * FROM int.award_id_lookup ORDER BY id, transaction_unique_id"
        delta_data = [row.asDict() for row in spark.sql(query).collect()]
        assert equal_datasets(expected_award_id_lookup, delta_data, "")
