"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from django.core.management import call_command
from usaspending_api.etl.tests.conftest import DELTA_LAKE_UNITTEST_SCHEMA_NAME
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC
from pyspark.sql import SparkSession


def _verify_delta_table_creation(spark: SparkSession, delta_table_name: str, s3_bucket: str):
    """Generic function that uses the create_delta_table command to create the given table and assert it was created
    as expected
    """
    delta_table_spec = TABLE_SPEC[delta_table_name]

    # NOTE: As of now, these tests allow the delta table to be created in a delta database (aka schema) as declared
    # in the table spec, rather than in the schema declared by DELTA_LAKE_UNITTEST_SCHEMA_NAME
    # This means they WILL NOT get removed/cleaned up from test run to test run. And may cause collisions
    # TODO: Fix the above, by forcing each table to be created in the unittest Delta Db schema
    assert delta_table_spec["destination_database"] != DELTA_LAKE_UNITTEST_SCHEMA_NAME

    call_command("create_delta_table", f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}")

    schemas = spark.sql("show schemas").collect()
    assert delta_table_spec["destination_database"] in [s["namespace"] for s in schemas]

    tables = spark.sql("show tables").collect()
    assert delta_table_name in [t["tableName"] for t in tables]
    the_delta_table = [
        t
        for t in tables
        if t["database"] == delta_table_spec["destination_database"] and t["tableName"] == delta_table_name
    ][0]
    assert the_delta_table["database"] == delta_table_spec["destination_database"]
    assert the_delta_table["isTemporary"] is False


def test_create_delta_table_for_sam_recipient(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "sam_recipient", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_lookup(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "recipient_lookup", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_profile(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "recipient_profile", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fabs(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "transaction_fabs", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fpds(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "transaction_fpds", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_search(spark, s3_unittest_data_bucket):
    _verify_delta_table_creation(spark, "transaction_search", s3_unittest_data_bucket)
