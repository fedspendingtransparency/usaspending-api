"""Automated Unit Tests for the loading of transaction and award tables in Delta Lake.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from django.core.management import call_command
from model_bakery import baker
from pytest import mark

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.etl.tests.integration.test_load_to_from_delta import load_delta_table_from_postgres, equal_datasets
from usaspending_api.etl.tests.integration.test_load_transactions_in_delta_lookups import (
    _BEGINNING_OF_TIME,
    _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
    _InitialRunWithPostgresLoader,
    _TableLoadInfo,
    TestInitialRun as InitialRun,  # Remove 'test' prefix to avoid pytest running these tests twice
    TestInitialRunNoPostgresLoader as InitialRunNoPostgresLoader,  # Remove 'test' prefix to avoid pytest running these tests twice
)
from usaspending_api.config import CONFIG
from usaspending_api.etl.management.commands.load_table_to_delta import TABLE_SPEC


class _TransactionFabsFpdsCore:

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
        # Setup some source tables without data, this test does not require these tables to be populated
        raw_db = "raw"
        self.spark.sql(f"create database if not exists {raw_db};")
        self.spark.sql(f"use {raw_db};")
        self.spark.sql(
            TABLE_SPEC["published_fabs"]["delta_table_create_sql"].format(
                DESTINATION_TABLE="published_fabs",
                DESTINATION_DATABASE=raw_db,
                SPARK_S3_BUCKET=self.s3_data_bucket,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
            )
        )
        self.spark.sql(
            TABLE_SPEC["detached_award_procurement"]["delta_table_create_sql"].format(
                DESTINATION_TABLE="detached_award_procurement",
                DESTINATION_DATABASE=raw_db,
                SPARK_S3_BUCKET=self.s3_data_bucket,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
            )
        )

        # 1. Call load_transactions_in_delta with etl-level of initial_run first, but without first loading
        # raw.transaction_normalized or raw.awards.  Then immediately call load_transactions_in_delta with
        # etl-level of transaction_f[ab|pd]s.
        InitialRun.initial_run(self.s3_data_bucket)
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the transaction and award id lookup tables and other int transaction tables.  They should all be empty.
        kwargs = {
            "expected_last_load_transaction_id_lookup": _BEGINNING_OF_TIME,
            "expected_last_load_award_id_lookup": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_normalized": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": _BEGINNING_OF_TIME,
        }
        # Even though nothing will have been loaded to that table, the table whose etl_level has been called will
        # have its last load date set to the date of the source tables' load.
        kwargs[f"expected_last_load_{self.etl_level}"] = _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        InitialRun.verify(self.spark, [], [], **kwargs)

        # 2. With raw.transaction_normalized and raw.awards still not created, call load_transactions_in_delta
        # with etl-level of transaction_id_lookup, and then again with etl-level of transaction_f[ab|pd]s.

        # Since the call to load_transactions_in_delta with etl-level of transaction_f[ab|pd]s above succeeded, we first
        # need to reset the last load date on transaction_fabs
        update_last_load_date(self.etl_level, _BEGINNING_OF_TIME)

        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # The expected transaction_id_lookup table should be the same as in _InitialRunWithPostgresLoader,
        # but all of the transaction ids should be 1 larger than expected there.
        expected_transaction_id_lookup = deepcopy(_InitialRunWithPostgresLoader.expected_initial_transaction_id_lookup)
        for item in expected_transaction_id_lookup:
            item["transaction_id"] += 1
        # Also, the last load date of the transaction_id_lookup table and of the table whose etl_level is being
        # called should be updated to the load time of the source tables
        kwargs["expected_last_load_transaction_id_lookup"] = _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        kwargs[f"expected_last_load_{self.etl_level}"] = _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        InitialRun.verify(
            self.spark,
            expected_transaction_id_lookup,
            [],
            0,
            len(self.expected_initial_transaction_fabs),
            len(self.expected_initial_transaction_fpds),
            **kwargs,
        )

        # Verify key fields in transaction_f[ab|pd]s table.  Note that the transaction_ids should be 1 more than
        # in those from _InitialRunWithPostgresLoader
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
        self, load_other_raw_tables, expected_initial_transaction_id_lookup, expected_initial_award_id_lookup
    ):
        # 1. Call load_transactions_in_delta with etl-level of initial_run first, making sure to load
        # raw.transaction_normalized along with the source tables, but don't copy the raw tables to int.
        # Then immediately call load_transactions_in_delta with etl-level of transaction_f[ab|pd]s.
        InitialRun.initial_run(self.s3_data_bucket, load_other_raw_tables=load_other_raw_tables, initial_copy=False)
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Even without the call to load_transactions_in_delta with etl-level of transaction_id_lookup, the appropriate
        # data will be populated in the transaction_id_lookup table via initial_run to allow the call to
        # load_transactions_in_delta with etl-level of transaction_fabs to populate int.transaction_fabs correctly with
        # the initial data.
        kwargs = {
            "expected_last_load_transaction_id_lookup": _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
            "expected_last_load_award_id_lookup": _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
            "expected_last_load_transaction_normalized": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_fabs": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": _BEGINNING_OF_TIME,
        }
        kwargs[f"expected_last_load_{self.etl_level}"] = _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        InitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            expected_initial_award_id_lookup,
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
        InitialRun.verify(
            self.spark,
            expected_initial_transaction_id_lookup,
            expected_initial_award_id_lookup,
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

    def unexpected_paths_no_pg_loader_test_core(self):
        self.unexpected_paths_test_core(
            [
                _TableLoadInfo(
                    self.spark,
                    "transaction_normalized",
                    InitialRunNoPostgresLoader.initial_transaction_normalized,
                )
            ],
            InitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            InitialRunNoPostgresLoader.expected_initial_award_id_lookup,
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
        InitialRun.initial_run(self.s3_data_bucket, load_other_raw_tables=load_other_raw_tables)
        call_command("load_transactions_in_delta", "--etl-level", "transaction_id_lookup")
        call_command("load_transactions_in_delta", "--etl-level", self.etl_level)

        # Verify the tables.  The transaction and award id lookup tables should be the same as during the initial run.
        # The transaction_normalized and transaction_f[ab|pd]s tables should have been copied from raw to int.
        kwargs = {
            "expected_last_load_transaction_id_lookup": _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
            "expected_last_load_award_id_lookup": _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
            "expected_last_load_transaction_normalized": _INITIAL_SOURCE_TABLE_LOAD_DATETIME,
            "expected_last_load_transaction_fabs": _BEGINNING_OF_TIME,
            "expected_last_load_transaction_fpds": _BEGINNING_OF_TIME,
        }
        kwargs[f"expected_last_load_{self.etl_level}"] = _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        InitialRun.verify(
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

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET place_of_perform_country_c = 'UNITED STATES'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET legal_entity_country_code = 'UNITED STATES'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET place_of_perform_country_n = 'USA'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
            """
        )

        self.spark.sql(
            f"""
                UPDATE raw.{self.usas_source_table_name}
                SET legal_entity_country_name = 'USA'
                WHERE {self.pk_field} = 4 OR {self.pk_field} = 5
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

        # Verify country code scalar transformation
        query = f"SELECT DISTINCT legal_entity_country_code, place_of_perform_country_c FROM int.{self.etl_level} WHERE {self.pk_field} = 4 OR {self.pk_field} = 5"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]
        assert len(delta_data) == 1
        assert delta_data[0]["legal_entity_country_code"] == "USA"
        assert delta_data[0]["place_of_perform_country_c"] == "USA"

        # Verify country name scalar transformation
        query = f"SELECT DISTINCT legal_entity_country_name, place_of_perform_country_n FROM int.{self.etl_level} WHERE {self.pk_field} = 4 OR {self.pk_field} = 5"
        delta_data = [row.asDict() for row in self.spark.sql(query).collect()]
        assert len(delta_data) == 1
        assert delta_data[0]["legal_entity_country_name"] == "UNITED STATES"
        assert delta_data[0]["place_of_perform_country_n"] == "UNITED STATES"

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

        # Verify that the last_load_dates of the transaction_id_lookup table and the table whose etl_level has been
        # called did NOT change, since only one of the broker source tables' last load date was changed.
        assert get_last_load_date("transaction_id_lookup") == _INITIAL_SOURCE_TABLE_LOAD_DATETIME
        assert get_last_load_date(self.etl_level) == _INITIAL_SOURCE_TABLE_LOAD_DATETIME

    def happy_paths_no_pg_loader_test_core(
        self,
        initial_transaction_fabs_fpds,
        expected_transaction_id_lookup_pops,
        expected_transaction_id_lookup_append,
        expected_transaction_fabs_fpds_append,
    ):
        self.happy_paths_test_core(
            (
                _TableLoadInfo(
                    self.spark,
                    "transaction_normalized",
                    InitialRunNoPostgresLoader.initial_transaction_normalized,
                ),
                _TableLoadInfo(
                    self.spark,
                    self.etl_level,
                    initial_transaction_fabs_fpds,
                ),
                _TableLoadInfo(self.spark, "awards", InitialRunNoPostgresLoader.initial_awards),
            ),
            InitialRunNoPostgresLoader.expected_initial_transaction_id_lookup,
            InitialRunNoPostgresLoader.expected_initial_award_id_lookup,
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
    compare_fields = _InitialRunWithPostgresLoader.expected_initial_transaction_fabs[0].keys()
    new_afa_generated_unique = "award_assist_0004_trans_0001"
    new_unique_award_key = "award_assist_0004"
    baker_kwargs = {
        "published_fabs_id": _TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
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
        "published_fabs_id": _TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "unique_award_key": new_unique_award_key.upper(),
    }

    def _generate_transaction_fabs_fpds_core(self, spark, s3_data_bucket, expected_initial_transaction_fabs):
        return _TransactionFabsFpdsCore(
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
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, _InitialRunWithPostgresLoader.expected_initial_transaction_fabs
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, InitialRunNoPostgresLoader.initial_transaction_fabs
        )
        transaction_fabs_fpds_core.unexpected_paths_no_pg_loader_test_core()

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, InitialRunNoPostgresLoader.initial_transaction_fabs
        )
        transaction_fabs_fpds_core.happy_paths_no_pg_loader_test_core(
            InitialRunNoPostgresLoader.initial_transaction_fabs,
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
    compare_fields = _InitialRunWithPostgresLoader.expected_initial_transaction_fpds[0].keys()
    new_detached_award_proc_unique = "award_procure_0004_trans_0001"
    new_unique_award_key = "award_procure_0004"
    baker_kwargs = {
        "detached_award_procurement_id": _TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "detached_award_proc_unique": new_detached_award_proc_unique,
        "unique_award_key": new_unique_award_key,
    }
    expected_transaction_id_lookup_append = {
        "is_fpds": True,
        "transaction_unique_id": new_detached_award_proc_unique.upper(),
    }
    expected_transaction_fabs_fpds_append = {
        "detached_award_proc_unique": new_detached_award_proc_unique.upper(),
        "detached_award_procurement_id": _TransactionFabsFpdsCore.new_transaction_fabs_fpds_id,
        "unique_award_key": new_unique_award_key.upper(),
    }

    def _generate_transaction_fabs_fpds_core(self, spark, s3_data_bucket, expected_initial_transaction_fpds):
        return _TransactionFabsFpdsCore(
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
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, _InitialRunWithPostgresLoader.expected_initial_transaction_fpds
        )
        transaction_fabs_fpds_core.unexpected_paths_source_tables_only_test_core()

    @mark.django_db(transaction=True)
    def test_unexpected_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, InitialRunNoPostgresLoader.initial_transaction_fpds
        )
        transaction_fabs_fpds_core.unexpected_paths_no_pg_loader_test_core()

    @mark.django_db(transaction=True)
    def test_happy_paths_no_pg_loader(
        self, spark, s3_unittest_data_bucket, hive_unittest_metastore_db, _populate_initial_source_tables_pg
    ):
        transaction_fabs_fpds_core = self._generate_transaction_fabs_fpds_core(
            spark, s3_unittest_data_bucket, InitialRunNoPostgresLoader.initial_transaction_fpds
        )
        transaction_fabs_fpds_core.happy_paths_no_pg_loader_test_core(
            InitialRunNoPostgresLoader.initial_transaction_fpds,
            (3, 4),
            self.expected_transaction_id_lookup_append,
            self.expected_transaction_fabs_fpds_append,
        )
