"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""

import json

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import psycopg2
import pytest
import pytz

from model_bakery import baker
from pyspark.sql import SparkSession

from django.conf import settings
from django.core.management import call_command
from django.db import connection, connections, transaction, models

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.etl.award_helpers import update_awards
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.commands.create_delta_table import (
    TABLE_SPEC,
)
from usaspending_api.etl.tests.integration.test_model import TestModel, TEST_TABLE_POSTGRES, TEST_TABLE_SPEC
from usaspending_api.recipient.models import RecipientLookup
from usaspending_api.tests.conftest_spark import create_and_load_all_delta_tables
from copy import deepcopy

_NEW_ASSIST = {
    "published_fabs_id": 6,
    "afa_generated_unique": "award_assist_0004_trans_0001",
    "is_active": True,
    "unique_award_key": "award_assist_0004",
    "uei": "CTKJDNGYLM97",
}
_NEW_PROCURE = {
    "detached_award_procurement_id": 6,
    "detached_award_proc_unique": "award_procure_0004_trans_0001",
    "unique_award_key": "award_procure_0004",
    "awardee_or_recipient_uei": "CTKJDNGYLM97",
}


def _handle_string_cast(val: str) -> Union[str, dict, list]:
    """
    JSON nested element columns are represented as JSON formatted strings in the Spark data, but nested elements in the
    Postgres data. Both columns will have a "custom_schema" defined for "<FIELD_TYPE> STRING". To handle this comparison
    the casting of values for "custom_schema" of STRING will attempt to convert a string to a nested element in the case
    of a nested element and fallback to a simple string cast.
    """
    if isinstance(val, list):
        try:
            casted = [json.loads(element) if isinstance(element, str) else element for element in val]
        except (TypeError, json.decoder.JSONDecodeError):
            casted = [str(element) for element in val]
    elif isinstance(val, dict):
        try:
            casted = {k: json.loads(element) if isinstance(element, str) else element for k, element in val.items()}
        except (TypeError, json.decoder.JSONDecodeError):
            casted = {k: str(element) for k, element in val.items()}
    else:
        try:
            casted = json.loads(val)
        except (TypeError, json.decoder.JSONDecodeError):
            casted = str(val)
    return casted


def sorted_deep(d):
    def make_tuple(v):
        if isinstance(v, list):
            return (*sorted_deep(v),)
        if isinstance(v, dict):
            return (*sorted_deep(list(v.items())),)
        return (v,)

    if isinstance(d, list):
        return sorted(map(sorted_deep, d), key=make_tuple)
    if isinstance(d, dict):
        return {k: sorted_deep(d[k]) for k in sorted(d)}
    return d


def equal_datasets(
    psql_data: List[Dict[str, Any]],
    spark_data: List[Dict[str, Any]],
    custom_schema: str,
    ignore_fields: Optional[list] = None,
):
    """Helper function to compare the two datasets. Note the column types of ds1 will be used to cast columns in ds2."""
    datasets_match = True

    # Parsing custom_schema to specify
    schema_changes = {}
    schema_type_converters = {"INT": int, "STRING": _handle_string_cast, "ARRAY<STRING>": _handle_string_cast}
    if custom_schema:
        for schema_change in custom_schema.split(", "):
            col, new_col_type = schema_change.split()[0].strip(), schema_change.split()[1].strip()
            schema_changes[col] = new_col_type

    # Iterating through the values and finding any differences
    for i, psql_row in enumerate(psql_data):
        for k, psql_val in psql_row.items():
            # Move on to the next value to check; ignoring this field
            if ignore_fields and k in ignore_fields:
                continue
            spark_val = spark_data[i][k]

            # Casting values based on the custom schema
            if (
                k.strip() in schema_changes
                and schema_changes[k].strip() in schema_type_converters
                and psql_val is not None
            ):
                spark_val = schema_type_converters[schema_changes[k].strip()](spark_val)
                psql_val = schema_type_converters[schema_changes[k].strip()](psql_val)

            # Equalize dates
            # - Postgres TIMESTAMPs may include time zones
            # - while Spark TIMESTAMPs will not
            #   - they are aligned to the Spark SQL session Time Zone (by way
            #     of spark.sql.session.timeZone conf setting) at the time they are read)
            #   - From: https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#timestamps-and-time-zones
            #   - "When writing timestamp values out to non-text data sources like Parquet, the values are just
            #      instants (like timestamp in UTC) that have no time zone information."
            if isinstance(psql_val, datetime):
                # Align to what Spark:
                #   1. Align the date to UTC, to shift the date the amount of time from the offset to UTC
                #   2. Then strip off the UTC time zone info
                utc_tz = pytz.timezone("UTC")
                psql_val_utc = psql_val.astimezone(utc_tz)
                psql_val = psql_val_utc.replace(tzinfo=None)

            # Make sure Postgres data is sorted in the case of a list since the Spark list data is sorted in ASC order
            if isinstance(psql_val, list):
                psql_val = sorted_deep(psql_val)
                if isinstance(spark_val, str):
                    spark_val = [json.loads(idx.replace("'", '"')) for idx in [spark_val]][0]
                spark_val = sorted_deep(spark_val)

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} "
                    f"left(psql):{psql_val} ({type(psql_val)}) "
                    f"right(spark):{spark_val} ({type(spark_val)})"
                )
    return datasets_match


def load_delta_table_from_postgres(
    delta_table_name: str,
    s3_bucket: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command: str = "load_table_to_delta",
):
    """Generic function that uses the create_delta_table and load_table_to_delta commands to create and load the
    given table
    """

    cmd_args = [f"--destination-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-db={alt_db}"]
    if alt_name:
        cmd_args += [f"--alt-name={alt_name}"]

    # make the table and load it
    call_command("create_delta_table", f"--spark-s3-bucket={s3_bucket}", *cmd_args)
    call_command(load_command, *cmd_args)


def verify_delta_table_loaded_to_delta(
    spark: SparkSession,
    delta_table_name: str,
    s3_bucket: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command: str = "load_table_to_delta",
    dummy_data: List[Dict[str, Any]] = None,
    ignore_fields: Optional[list] = None,
):
    """Generic function that uses the create_delta_table, load_table_to_delta, and load_query_to_delta commands to
    create and load the given table and assert it was created and loaded as expected
    """

    if delta_table_name == "summary_state_view":
        cmd_args = [f"--destination-table={delta_table_name}"]
        if alt_db:
            cmd_args += [f"--alt-db={alt_db}"]
        if alt_name:
            cmd_args += [f"--alt-name={alt_name}"]

        # Create the table and load the query to delta
        call_command("create_delta_table", f"--spark-s3-bucket={s3_bucket}", *cmd_args)
        call_command(load_command, *cmd_args)
    else:
        load_delta_table_from_postgres(delta_table_name, s3_bucket, alt_db, alt_name, load_command)

    if alt_name:
        expected_table_name = alt_name
    else:
        expected_table_name = delta_table_name.split(".")[-1]

    partition_col = TABLE_SPEC[delta_table_name].get("partition_column")
    if dummy_data is None:
        # get the postgres data to compare
        model = TABLE_SPEC[delta_table_name]["model"]
        is_from_broker = TABLE_SPEC[delta_table_name]["is_from_broker"]
        if delta_table_name == "summary_state_view":
            dummy_query = f"SELECT * from {expected_table_name}"
            if partition_col is not None:
                dummy_query = f"{dummy_query} ORDER BY {partition_col}"
            dummy_data = [row.asDict() for row in spark.sql(dummy_query).collect()]
        elif model:
            dummy_query = model.objects
            if partition_col is not None:
                dummy_query = dummy_query.order_by(partition_col)
            dummy_data = list(dummy_query.all().values())
        elif is_from_broker:
            # model can be None if loading from the Broker
            broker_connection = connections[settings.DATA_BROKER_DB_ALIAS]
            source_broker_name = TABLE_SPEC[delta_table_name]["source_table"]
            with broker_connection.cursor() as cursor:
                dummy_query = f"SELECT * from {source_broker_name}"
                if partition_col is not None:
                    dummy_query = f"{dummy_query} ORDER BY {partition_col}"
                cursor.execute(dummy_query)
                dummy_data = dictfetchall(cursor)
        else:
            raise ValueError(
                "No dummy data nor model provided and the table is not from the Broker. Please provide one"
                "of these for the test to compare the data."
            )

    # get the spark data to compare
    # NOTE: The ``use <db>`` from table create/load is still in effect for this verification. So no need to call again
    received_query = f"SELECT * from {expected_table_name}"
    if partition_col is not None:
        received_query = f"{received_query} ORDER BY {partition_col}"
    received_data = [row.asDict() for row in spark.sql(received_query).collect()]

    assert equal_datasets(dummy_data, received_data, TABLE_SPEC[delta_table_name]["custom_schema"], ignore_fields)


def verify_delta_table_loaded_from_delta(
    spark: SparkSession,
    delta_table_name: str,
    alt_db: str = None,
    alt_name: str = None,
    load_command="load_table_from_delta",
    jdbc_inserts: bool = False,
    spark_s3_bucket: str = None,
    ignore_fields: Optional[list] = None,
):
    """Generic function that uses the load_table_from_delta commands to load the given table and assert it was
    downloaded as expected
    """
    cmd_args = [f"--delta-table={delta_table_name}"]
    if alt_db:
        cmd_args += [f"--alt-delta-db={alt_db}"]
    expected_table_name = delta_table_name
    if alt_name:
        cmd_args += [f"--alt-delta-name={alt_name}"]
        expected_table_name = alt_name
    if jdbc_inserts:
        cmd_args += [f"--jdbc-inserts"]
    else:
        if not spark_s3_bucket:
            raise RuntimeError(
                "spark_s3_bucket=None. A unit test S3 bucket needs to be created and provided for this test."
            )
        cmd_args += [f"--spark-s3-bucket={spark_s3_bucket}"]

    # table already made, let's load it
    call_command(load_command, *cmd_args)

    # get the postgres data to compare
    source_table = TABLE_SPEC[delta_table_name]["source_table"] or TABLE_SPEC[delta_table_name]["swap_table"]
    temp_schema = "temp"
    if source_table:
        tmp_table_name = f"{temp_schema}.{source_table}_temp"
    else:
        tmp_table_name = f"{temp_schema}.{expected_table_name}_temp"
    postgres_query = f"SELECT * FROM {tmp_table_name}"
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    if partition_col is not None:
        postgres_query = f"{postgres_query} ORDER BY {partition_col}"
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(postgres_query)
            postgres_data = dictfetchall(cursor)

    # get the spark data to compare
    delta_query = f"SELECT * FROM {expected_table_name}"
    if partition_col is not None:
        delta_query = f"{delta_query} ORDER BY {partition_col}"
    delta_data = [row.asDict() for row in spark.sql(delta_query).collect()]

    assert equal_datasets(
        postgres_data, delta_data, TABLE_SPEC[delta_table_name]["custom_schema"], ignore_fields=ignore_fields
    )


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_from_delta_for_recipient_lookup(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
    # Postgres table, and then push the updated table to Delta.
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    ignore_fields = ["id", "update_date"]
    tables_to_load = ["sam_recipient", "transaction_fabs", "transaction_fpds", "transaction_normalized"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)

    # Test initial load of Recipient Lookup
    call_command("update_recipient_lookup")
    verify_delta_table_loaded_to_delta(
        spark,
        "recipient_lookup",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=ignore_fields,
    )

    # Create a new Transaction a transaction that represents a new name for a recipient
    new_award = baker.make(
        "search.AwardSearch",
        award_id=1000,
        type="07",
        period_of_performance_start_date="2021-01-01",
        period_of_performance_current_end_date="2022-01-01",
        date_signed="2021-01-01",
        total_obligation=100.00,
        total_subsidy_cost=100.00,
        type_description="Direct Loan",
        subaward_count=0,
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=1001,
        afa_generated_unique=1001,
        action_date="2021-01-01",
        fiscal_action_date="2021-04-01",
        award_id=new_award.award_id,
        is_fpds=False,
        type="07",
        last_modified_date="2021-01-01",
        cfda_number="12.456",
        recipient_uei="FABSUEI12345",
        recipient_unique_id="FABSDUNS12345",
        recipient_name="ALTERNATE NAME RECIPIENT",
        recipient_name_raw="ALTERNATE NAME RECIPIENT",
        parent_uei="PARENTUEI12345",
        parent_recipient_unique_id="PARENTDUNS12345",
        parent_recipient_name="PARENT RECIPIENT 12345",
        parent_recipient_name_raw="PARENT RECIPIENT 12345",
        indirect_federal_sharing=1.0,
        total_funding_amount="2.23",
        recipient_location_state_code="VA",
        recipient_location_county_code="001",
        recipient_location_country_code="USA",
        recipient_location_country_name="UNITED STATES",
        recipient_location_congressional_code="01",
        recipient_location_congressional_code_current=None,
        pop_state_code="VA",
        pop_county_code="001",
        pop_country_code="USA",
        pop_country_name="UNITED STATES",
        pop_congressional_code="01",
        pop_congressional_code_current=None,
    )

    update_awards()

    # Test that the following load correctly merges
    call_command("update_recipient_lookup")

    # Verify that the update alternate name exists
    expected_result = ["FABS RECIPIENT 12345"]
    assert sorted(RecipientLookup.objects.filter(uei="FABSUEI12345").first().alternate_names) == expected_result

    tables_to_load = ["transaction_fabs", "transaction_normalized"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "recipient_lookup",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=ignore_fields,
    )
    verify_delta_table_loaded_from_delta(
        spark, "recipient_lookup", spark_s3_bucket=s3_unittest_data_bucket, ignore_fields=ignore_fields
    )
    verify_delta_table_loaded_from_delta(
        spark, "recipient_lookup", jdbc_inserts=True, ignore_fields=ignore_fields
    )  # test alt write strategy


@pytest.mark.django_db(transaction=True)
def test_load_table_to_delta_for_published_fabs(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    baker.make(
        "transactions.SourceAssistanceTransaction",
        published_fabs_id=7,
        created_at=datetime.fromtimestamp(0),
        modified_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        indirect_federal_sharing=22.00,
        is_active=True,
        federal_action_obligation=1000001,
        face_value_loan_guarantee=22.00,
        non_federal_funding_amount=44.00,
        original_loan_subsidy_cost=55.00,
        submission_id=33.00,
        _fill_optional=True,
    )
    verify_delta_table_loaded_to_delta(spark, "published_fabs", s3_unittest_data_bucket)


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_from_delta_for_recipient_profile(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
    # Postgres table, and then push the updated table to Delta.
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "sam_recipient",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "recipient_profile", s3_unittest_data_bucket, load_command="load_query_to_delta", ignore_fields=["id"]
    )
    verify_delta_table_loaded_from_delta(spark, "recipient_profile", jdbc_inserts=True, ignore_fields=["id"])


@pytest.mark.django_db(transaction=True)
def test_load_table_to_delta_timezone_aware(spark, monkeypatch, s3_unittest_data_bucket, hive_unittest_metastore_db):
    """Test that timestamps are not inadvertently shifted due to loss of timezone during reads and writes.

    The big takeaways from this are:
    BLUF: Keep Django oriented to UTC (we're doing this) and keep the same timezone set in spark sessions where you
    are reading and writing data (we're doing this)

    1. Postgres stores its fields with data type ``timestamp with time zone`` under the hood in UTC, and it's up to
       the connection (and the ``time zone`` configuration parameter on that connection) to determine how it wants to
       LOOK AT that timezone-aware data (from what time zone perspective).
    2. All the django reads/writes are done via a UTC connection, because we use USE_TZ=True and TIME_ZONE="UTC" in
       settings.py
    3. But even if it weren't, it doesn't matter, since the data is stored in UTC in postgres regardless of what the
       settings where
    4. Where that Django conn time zone does matter, is if we naively strip off the timezone from a timezone-aware
       datetime value that was read under non-UTC settings, and assume its value is UTC-aligned. But we're luckily
       protected from any naivete like this by those settings.py settings.
    5. Spark will store Parquet data as an instant, without any timezone information. It's up to the session
       in which the data is being read to infer any timezone part on that instant. This is governed by the
       spark.sql.session.timeZone setting. If you read and write data with the same session timeZone, all is good (
       seeing it from the same perspective). But if you were to write it with one time zone and read it with another,
       it would be inadvertently adding/removing hours from the stored time instant (as it was written).
    """
    # Add another record with explict timezone on it
    tz_hst = pytz.timezone("HST")  # Hawaii–Aleutian Standard Time = UTC-10:00
    tz_utc = pytz.timezone("UTC")
    dt_naive = datetime(2022, 6, 11, 11, 11, 11)
    dt_with_tz = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_hst)
    dt_with_utc = datetime(2022, 6, 11, 11, 11, 11, tzinfo=tz_utc)
    # Because if they are reflecting the same day+hour, then the HST (further east/back) TZ with a -10
    # offset is actually 10 hours ahead of the stated day+hour when looked at in the UTC timezone
    assert dt_with_utc.timestamp() < dt_with_tz.timestamp()

    # Setting up an agnostic TestModel and updating the TABLE_SPEC
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute(TEST_TABLE_POSTGRES)
    TABLE_SPEC.update(TEST_TABLE_SPEC)
    monkeypatch.setattr("usaspending_api.etl.management.commands.load_table_to_delta.TABLE_SPEC", TABLE_SPEC)

    # Prepare a model object without saving it, but do save the related fields
    # - https://model-bakery.readthedocs.io/en/latest/basic_usage.html#non-persistent-objects
    # Do this so we can save the TransactionFABS record without interference from the Django DB connections
    # Session settings (like sesssion-set time zone)
    model_with_tz = baker.prepare(
        TestModel,
        _save_related=True,
        id=3,
        test_timestamp=dt_with_tz,
    )  # type: TestModel
    populated_columns = ("id", "test_timestamp")

    def _get_sql_insert_from_model(model, populated_columns):
        values = [value for value in model._meta.local_fields if value.column in populated_columns]
        q = models.sql.InsertQuery(model)
        q.insert_values(values, [model])
        compiler = q.get_compiler("default")
        setattr(compiler, "return_id", False)
        stmts = compiler.as_sql()
        stmt = [
            stmt % tuple(f"'{param}'" if type(param) in [str, date, datetime] else param for param in params)
            for stmt, params in stmts
        ]
        return stmt[0]

    # Now save it to the test DB using a new connection, that establishes its own time zone during it session
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            fabs_insert_sql = _get_sql_insert_from_model(model_with_tz, populated_columns)
            cursor.execute(fabs_insert_sql)
            assert cursor.rowcount == 1
            new_psycopg2_conn.commit()

    # See how things look from Django's perspective
    with transaction.atomic():
        # Fetch the DB object in a new transaction
        test_model_with_tz = TestModel.objects.filter(id=3).first()
        assert test_model_with_tz is not None

        # Check that all dates are as expected
        model_datetime = test_model_with_tz.test_timestamp  # type: datetime
        assert model_datetime.tzinfo is not None

        # NOTE: this is because of our Django settings. Upon saving timezone-aware data, it shifts it to UTC
        # Specifically, in settings.py, you will find
        #   TIME_ZONE = "UTC"
        #   USE_TZ = True
        # And this value STICKS in the long-lived django ``connection`` object. So long as that is used (with the ORM
        # or with raw SQL), it will apply those time zone settings
        assert model_datetime.tzname() != "HST"
        assert model_datetime.tzname() == "UTC"
        assert model_datetime.hour == 21  # shifted +10 to counteract the UTC offset by django upon saving it
        assert model_datetime.utctimetuple().tm_hour == 21  # already shifted to UTC, so this just matches .hour (== 21)
        assert dt_naive.utctimetuple().tm_hour == dt_naive.hour  # naive, so stays the same
        assert dt_with_utc.utctimetuple().tm_hour == dt_with_utc.hour  # already UTC, so stays the same

        # Confirm also that this is the case in the DB (i.e. it was at write-time that UTC was set, not read-time
        with connection.cursor() as cursor:
            cursor.execute("select test_table.test_timestamp from test_table where id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            assert dt_from_db.tzname() == "UTC"

        # Confirm whether the Test DB created was forced to UTC timezone based on settings.py
        # (Spoiler, yes it does)
        with connection.cursor() as cursor:
            cursor.execute("show time zone")
            db_tz = [row[0] for row in cursor.fetchall()][0]
            assert db_tz is not None
            assert db_tz == "UTC"

    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            cursor.execute("select test_table.test_timestamp from test_table where id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            # Can't use traditional time zone names with tzname() since pyscopg2 uses its own time zone infos.
            # Use psycopg2 tzinfo name and then compare their delta
            assert dt_from_db.tzname() == "UTC-10:00"
            assert dt_from_db.utcoffset().total_seconds() == -36000.0

    # Now with that DB data committed, and with the DB set to HST TIME ZONE, do a Spark read
    try:
        # Hijack the spark time zone setting for the purposes of this test to make it NOT UTC
        # This should not matter so long as the data is WRITTEN (e.g. to parquet)
        # and READ (e.g. from a Delta Table over that parquet into a DataFrame) under the SAME timezone
        original_spark_tz = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        verify_delta_table_loaded_to_delta(spark, "test_table", s3_unittest_data_bucket)
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_spark_tz)
        with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
            with new_psycopg2_conn.cursor() as cursor:
                cursor.execute("DROP TABLE test_table")


@pytest.mark.django_db(transaction=True)
def test_load_table_to_delta_for_detached_award_procurement(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id="4",
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )
    baker.make(
        "transactions.SourceProcurementTransaction",
        detached_award_procurement_id="5",
        created_at=datetime.fromtimestamp(0),
        updated_at=datetime.fromtimestamp(0),
        federal_action_obligation=1000001,
        _fill_optional=True,
    )

    verify_delta_table_loaded_to_delta(spark, "detached_award_procurement", s3_unittest_data_bucket)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skip(reason="Due to the nature of the views with all the transformations, this will be out of date")
def test_load_table_to_from_delta_for_recipient_profile_testing(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    tables_to_load = [
        "recipient_lookup",
        "sam_recipient",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "recipient_profile_testing", s3_unittest_data_bucket, load_command="load_table_to_delta"
    )


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_from_delta_for_transaction_search(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
    # Postgres table, and then push the updated table to Delta.
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_search",
        s3_unittest_data_bucket,
        load_command="load_query_to_delta",
        ignore_fields=["award_update_date", "etl_update_date"],
    )
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_from_delta(spark, "transaction_search", spark_s3_bucket=s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(spark, "transaction_search", jdbc_inserts=True)  # test alt write strategy


@pytest.mark.skip(
    reason="Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC "
    "as by design the data in delta will be different from the data in postgres"
)
@pytest.mark.django_db(transaction=True)
def test_load_table_to_from_delta_for_transaction_search_testing(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_to_delta(spark, "transaction_search_testing", s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(spark, "transaction_search_testing", spark_s3_bucket=s3_unittest_data_bucket)
    # verify_delta_table_loaded_from_delta(
    #     spark, "transaction_search_testing", jdbc_inserts=True
    # )  # test alt write strategy
    pass


@pytest.mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_normalized_alt_db_and_name(
    spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):
    baker.make("search.TransactionSearch", transaction_id="1", award_id=1, _fill_optional=True)
    baker.make("search.TransactionSearch", transaction_id="2", award_id=2, _fill_optional=True)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_normalized",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_normalized_alt_name",
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.skip(reason="Due to the nature of the views with all the transformations, this will be out of date")
def test_load_table_to_from_delta_for_transaction_search_alt_db_and_name(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark,
        "transaction_search",
        s3_unittest_data_bucket,
        alt_db="my_alt_db",
        alt_name="transaction_search_alt_name",
        load_command="load_query_to_delta",
    )
    # TODO: Commenting these out while we have `transaction_search_gold` vs `transaction_search` in the TABLE_SPEC
    #       as by design the data in delta will be different from the data in postgres
    # verify_delta_table_loaded_from_delta(
    #     spark,
    #     "transaction_search",
    #     alt_db="my_alt_db",
    #     alt_name="transaction_search_alt_name",
    #     spark_s3_bucket=s3_unittest_data_bucket,
    # )


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_from_delta_for_award_search(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Since changes to the source tables will go to the Postgres table first, use model baker to add new rows to
    # Postgres table, and then push the updated table to Delta.
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "award_search", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    verify_delta_table_loaded_from_delta(spark, "award_search", spark_s3_bucket=s3_unittest_data_bucket)
    verify_delta_table_loaded_from_delta(spark, "award_search", jdbc_inserts=True)  # test alt write strategy


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_incremental_load_table_to_delta_for_award_search(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Load in data that award_search depends on
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)

    # create award_search table with cdf enabled
    call_command(
        "create_delta_table",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
        f"--destination-table=award_search",
        "--alt-db=int",
    )

    # load in award_search data
    call_command(
        "load_query_to_delta",
        f"--destination-table=award_search",
        "--incremental",
        "--alt-db=int",
    )

    # Delete one of the awards
    spark.sql("DELETE FROM int.award_search WHERE award_id = 4")

    # Reload the data
    call_command(
        "load_query_to_delta",
        f"--destination-table=award_search",
        "--incremental",
        "--alt-db=int",
    )

    result = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table("award_search")
        .select(["award_id", "_change_type", "_commit_version"])
        .toPandas()
    )
    expected = pd.DataFrame(
        {
            "award_id": [4, 4, 1, 3, 2, 4],
            "_change_type": ["delete", "insert", "insert", "insert", "insert", "insert"],
            "_commit_version": [2, 3, 1, 1, 1, 1],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_incremental_load_table_to_delta_for_transaction_search(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):
    # Load in data that transaction_search depends on
    last_load_datetime = datetime.now(timezone.utc)
    insert_datetime = last_load_datetime + timedelta(minutes=-15)
    assist = deepcopy(_NEW_ASSIST)
    assist.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceAssistanceTransaction", **assist)
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)

    procure = deepcopy(_NEW_PROCURE)
    procure.update(
        {"action_date": insert_datetime.isoformat(), "created_at": insert_datetime, "updated_at": insert_datetime}
    )
    baker.make("transactions.SourceProcurementTransaction", **procure)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)

    # create award_search table with cdf enabled
    call_command(
        "create_delta_table",
        f"--spark-s3-bucket={s3_unittest_data_bucket}",
        f"--destination-table=transaction_search",
        "--alt-db=int",
    )

    # load in award_search data
    call_command(
        "load_query_to_delta",
        f"--destination-table=transaction_search",
        "--incremental",
        "--alt-db=int",
    )

    # Delete one of the awards
    spark.sql("DELETE FROM int.transaction_search WHERE transaction_id = 4")

    # Reload the data
    call_command(
        "load_query_to_delta",
        f"--destination-table=transaction_search",
        "--incremental",
        "--alt-db=int",
    )

    result = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table("transaction_search")
        .select(["transaction_id", "_change_type", "_commit_version"])
        .toPandas()
    )
    expected = pd.DataFrame(
        {
            "transaction_id": [4, 4, 1, 2, 434, 3, 4, 5],
            "_change_type": ["delete", "insert", "insert", "insert", "insert", "insert", "insert", "insert"],
            "_commit_version": [2, 3, 1, 1, 1, 1, 1, 1],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_delta_for_sam_recipient(spark, s3_unittest_data_bucket, populate_broker_data):
    expected_data = [
        {
            "awardee_or_recipient_uniqu": "812918241",
            "legal_business_name": "EL COLEGIO DE LA FRONTERA SUR",
            "dba_name": "RESEARCH CENTER",
            "ultimate_parent_unique_ide": "811979236",
            "ultimate_parent_legal_enti": "GOBIERNO FEDERAL DE LOS ESTADOS UNIDOS MEXICANOS",
            "address_line_1": "CALLE 10 NO. 264, ENTRE 61 Y 63",
            "address_line_2": "",
            "city": "CAMPECHE",
            "state": "CAMPECHE",
            "zip": "24000",
            "zip4": None,
            "country_code": "MEX",
            "congressional_district": None,
            "business_types_codes": ["20", "2U", "GW", "M8", "V2"],
            "entity_structure": "X6",
            "broker_duns_id": "1",
            "update_date": date(2015, 2, 5),
            "uei": "CTKJDNGYLM97",
            "ultimate_parent_uei": "KDULNMSMR7E6",
        }
    ]
    verify_delta_table_loaded_to_delta(
        spark, "sam_recipient", s3_unittest_data_bucket, load_command="load_query_to_delta", dummy_data=expected_data
    )


@pytest.mark.skipif(
    settings.DATA_BROKER_DB_ALIAS not in settings.DATABASES,
    reason="'data_broker' database not configured in django settings.DATABASES.",
)
@pytest.mark.django_db(databases=[settings.DATA_BROKER_DB_ALIAS, settings.DEFAULT_DB_ALIAS], transaction=True)
def test_load_table_to_delta_for_summary_state_view(
    spark, s3_unittest_data_bucket, populate_usas_data_and_recipients_from_broker, hive_unittest_metastore_db
):

    # We need the award_search table to create the summary_state_view in delta
    # And in order to create the award_search table, we need the following
    load_delta_table_from_postgres("published_fabs", s3_unittest_data_bucket)
    load_delta_table_from_postgres("detached_award_procurement", s3_unittest_data_bucket)

    tables_to_load = [
        "awards",
        "financial_accounts_by_awards",
        "recipient_lookup",
        "recipient_profile",
        "sam_recipient",
        "transaction_current_cd_lookup",
        "transaction_fabs",
        "transaction_fpds",
        "transaction_normalized",
        "zips",
    ]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "award_search", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )

    # We now want to load the award_search table that we created above along with other tables needed to create award_search
    # Then create the summay_state_view table and populate it using the load_query_to_delta command
    tables_to_load = ["transaction_fabs", "transaction_fpds", "transaction_normalized", "award_search"]
    create_and_load_all_delta_tables(spark, s3_unittest_data_bucket, tables_to_load)
    verify_delta_table_loaded_to_delta(
        spark, "summary_state_view", s3_unittest_data_bucket, load_command="load_query_to_delta"
    )
    # Lastly, check using verify_delta_table_loaded_from_delta function which will run the load_table_from_delta command
    verify_delta_table_loaded_from_delta(spark, "summary_state_view", spark_s3_bucket=s3_unittest_data_bucket)
