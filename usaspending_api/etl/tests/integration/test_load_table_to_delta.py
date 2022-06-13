"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from datetime import datetime
from typing import List, Dict, Any

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import mark
from django.core.management import call_command

from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


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

            # Equalize dates - Postgres TIMESTAMPs may include time zones while the Spark TIMESTAMPs may not
            # TODO: Initial implementation by naively stripping off timezone info and comparing core date.
            #  Eventually refactor to normalize timezone-aware datetime objects from psql to UTC, but shifting their
            #  core date and time to UTC before/after stripping timezone info
            if isinstance(psql_val, datetime):
                psql_val = psql_val.replace(tzinfo=None)

            if psql_val != spark_val:
                raise Exception(
                    f"Not equal: col:{k} " f"left:{psql_val}({psql_val_type}) " f"right:{spark_val}({type(spark_val)})"
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
def test_load_table_to_delta_for_transaction_fpds(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="1", _fill_optional=True)
    baker.make("awards.TransactionFPDS", detached_award_procurement_id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_fpds", s3_unittest_data_bucket)


@mark.django_db(transaction=True)
def test_load_table_to_delta_for_transaction_normalized(spark, s3_unittest_data_bucket):
    baker.make("awards.TransactionNormalized", id="1", _fill_optional=True)
    baker.make("awards.TransactionNormalized", id="2", _fill_optional=True)
    _verify_delta_table_loaded(spark, "transaction_normalized", s3_unittest_data_bucket)
