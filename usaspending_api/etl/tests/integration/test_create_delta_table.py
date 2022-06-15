"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
from pyspark.sql import SparkSession

from django.core.management import call_command

from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


def _verify_delta_table_creation(spark: SparkSession, delta_table_name: str, s3_bucket: str):
    """Generic function that uses the create_delta_table command to create the given table and assert it was created
    as expected

    NOTE: If calling test is using the hive_unittest_metastore_db fixture, these tables will be blown away after
    each test (and the metastore_db itself will be blown away after the full test session)
    """
    delta_table_spec = TABLE_SPEC[delta_table_name]

    call_command("create_delta_table", f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}")

    schemas = spark.sql("show schemas").collect()
    assert delta_table_spec["destination_database"] in [s["namespace"] for s in schemas]

    tables = spark.sql("show tables").collect()
    assert delta_table_name in [t["tableName"] for t in tables]
    the_delta_table = [
        t
        for t in tables
        if t["namespace"] == delta_table_spec["destination_database"] and t["tableName"] == delta_table_name
    ][0]
    assert the_delta_table["namespace"] == delta_table_spec["destination_database"]
    assert the_delta_table["isTemporary"] is False


def test_create_delta_table_for_award_search(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "award_search", s3_unittest_data_bucket)


def test_create_delta_table_for_awards(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "awards", s3_unittest_data_bucket)


def test_create_delta_table_for_financial_accounts_by_awards(
    spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):
    _verify_delta_table_creation(spark, "financial_accounts_by_awards", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_lookup(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "recipient_lookup", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_profile(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "recipient_profile", s3_unittest_data_bucket)


def test_create_delta_table_for_sam_recipient(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "sam_recipient", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fabs(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_fabs", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fpds(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_fpds", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_normalized(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_normalized", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_search_testing(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    """
    The actual transaction_search create is tested with load commands because data is loaded in during the
    creation of the table via 'CREATE TABLE ... SELECT AS ...'. This test is used for the
    'transaction_search_testing' table which is a direct copy of the current 'transaction_search' Postgres table
    to aid in testing.
    TODO: Remove this test and relevant 'transaction_search_testing' code when we go to copy over transaction_search
    """
    _verify_delta_table_creation(spark, "transaction_search_testing", s3_unittest_data_bucket)
