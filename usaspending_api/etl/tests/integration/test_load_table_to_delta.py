"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import psycopg2
import pytz
from datetime import datetime
from typing import List, Dict, Any

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import mark
from django.core.management import call_command
from django.db import connection, transaction
from django.db.models import sql

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC
from usaspending_api.awards.models import TransactionFABS


def equal_datasets(psql_data: List[Dict[str, Any]], spark_data: List[Dict[str, Any]], custom_schema: str):
    """Helper function to compare the two datasets. Note the column types of ds1 will be used to cast columns in ds2."""
    datasets_match = True

    # Parsing custom_schema to specify
    schema_changes = {}
    schema_type_converters = {
        "INT": int,
        "STRING": str,
    }
    if custom_schema:
        for schema_change in custom_schema.split(","):
            col, new_col_type = schema_change.split()[0].strip(), schema_change.split()[1].strip()
            schema_changes[col] = new_col_type

    # Iterating through the values and finding any differences
    for i, psql_row in enumerate(psql_data):
        for k, psql_val in psql_row.items():
            psql_val_type = type(psql_val)
            spark_val = spark_data[i][k]

            # Casting the postgres values based on the custom schema
            if k.strip() in schema_changes and schema_changes[k].strip() in schema_type_converters:
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

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} "
                    f"left(psql):{psql_val} ({psql_val_type}) "
                    f"right(spark):{spark_val} ({type(spark_val)})"
                )
    return datasets_match


def _verify_delta_table_loaded(spark: SparkSession, delta_table_name: str, s3_bucket: str):
    """Generic function that uses the create_delta_table and load_table_to_delta ommands to create and load the given
    table and assert it was created and loaded as expected
    """
    # make the table and load it
    call_command("create_delta_table", f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}")
    call_command("load_table_to_delta", f"--destination-table={delta_table_name}")

    # get the postgres data to compare
    model = TABLE_SPEC[delta_table_name]["model"]
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    dummy_query = model.objects
    if partition_col is not None:
        dummy_query = dummy_query.order_by(partition_col)
    dummy_data = list(dummy_query.all().values())

    # get the spark data to compare
    received_query = f"select * from {delta_table_name}"
    if partition_col is not None:
        received_query = f"{received_query} order by {partition_col}"
    received_data = [row.asDict() for row in spark.sql(received_query).collect()]

    assert equal_datasets(dummy_data, received_data, TABLE_SPEC[delta_table_name]["custom_schema"])


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_sam_recipient(spark, s3_unittest_data_bucket):
    baker.make("recipient.DUNS", broker_duns_id="1", _fill_optional=True)
    baker.make("recipient.DUNS", broker_duns_id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "sam_recipient", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_recipient_lookup(spark, s3_unittest_data_bucket):
    baker.make("recipient.RecipientLookup", id="1", _fill_optional=True)
    baker.make("recipient.RecipientLookup", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "recipient_lookup", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_recipient_profile(spark, s3_unittest_data_bucket):
    baker.make("recipient.RecipientProfile", id="1", _fill_optional=True)
    baker.make("recipient.RecipientProfile", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "recipient_profile", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_fabs(spark, s3_unittest_data_bucket):
    # Baker doesn't support autofilling Numeric fields, so we're manually setting them here
    baker.make("awards.TransactionFABS", published_fabs_id="1", indirect_federal_sharing=1.0, _fill_optional=True)
    baker.make("awards.TransactionFABS", published_fabs_id="2", indirect_federal_sharing=1.0, _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_fabs", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_fabs_timezone_aware(spark, s3_unittest_data_bucket):
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

    # Prepare a model object without saving it, but do save the related fields
    # - https://model-bakery.readthedocs.io/en/latest/basic_usage.html#non-persistent-objects
    # Do this so we can save the TransactionFABS record without interference from the Django DB connections
    # Session settings (like sesssion-set time zone)
    fabs_with_tz = baker.prepare(
        "awards.TransactionFABS",
        _save_related=True,
        published_fabs_id="3",
        indirect_federal_sharing=1.0,
        modified_at=dt_with_tz,
        _fill_optional=True,
    )  # type: TransactionFABS

    def _get_sql_insert_from_model(model):
        values = model._meta.local_fields
        q = sql.InsertQuery(model)
        q.insert_values(values, [model])
        compiler = q.get_compiler("default")
        setattr(compiler, "return_id", False)
        stmts = compiler.as_sql()
        stmt = [
            stmt % tuple(f"'{param}'" if type(param) in [str, datetime] else param for param in params)
            for stmt, params in stmts
        ]
        return stmt[0]

    # Now save it to the test DB using a new connection, that establishes its own time zone during it session
    with psycopg2.connect(get_database_dsn_string()) as new_psycopg2_conn:
        with new_psycopg2_conn.cursor() as cursor:
            cursor.execute("set session time zone 'HST'")
            fabs_insert_sql = _get_sql_insert_from_model(fabs_with_tz)
            cursor.execute(fabs_insert_sql)
            assert cursor.rowcount == 1
            new_psycopg2_conn.commit()

    # See how things look from Django's perspective
    with transaction.atomic():
        # Fetch the DB object in a new transaction
        fabs_with_tz = TransactionFABS.objects.filter(published_fabs_id="3").first()
        assert fabs_with_tz is not None

        # Check that all dates are as expected
        model_datetime = fabs_with_tz.modified_at  # type: datetime
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
            cursor.execute("select modified_at from transaction_fabs where published_fabs_id = 3")
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
            cursor.execute("select modified_at from transaction_fabs where published_fabs_id = 3")
            dt_from_db = [row[0] for row in cursor.fetchall()][0]  # type: datetime
            assert dt_from_db.tzinfo is not None
            # Can't use traditional time zone names with tzname() since pyscopg2 uses its own time zone infos.
            # Use psycopg2 tzinfo name and then compare their delta
            assert dt_from_db.tzname() == "-10"
            assert dt_from_db.utcoffset().total_seconds() == -36000.0

    # Now with that DB data committed, and with the DB set to HST TIME ZONE, do a Spark read
    try:
        # Hijack the spark time zone setting for the purposes of this test to make it NOT UTC
        # This should not matter so long as the data is WRITTEN (e.g. to parquet)
        # and READ (e.g. from a Delta Table over that parquet into a DataFrame) under the SAME timezone
        original_spark_tz = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        _verify_delta_table_loaded(spark, "transaction_fabs", s3_unittest_data_bucket)
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_spark_tz)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_fpds(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="1", _fill_optional=True)
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_fpds", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_normalized(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionNormalized", id="1", _fill_optional=True)
    baker.make("awards.TransactionNormalized", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_normalized", s3_unittest_data_bucket)
