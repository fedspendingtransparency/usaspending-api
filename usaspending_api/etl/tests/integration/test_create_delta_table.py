"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""

from pyspark.sql import SparkSession

from django.core.management import call_command

from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


def _verify_delta_table_creation(
    spark: SparkSession, delta_table_name: str, s3_bucket: str, alt_db: str = None, alt_name: str = None
):
    """Generic function that uses the create_delta_table command to create the given table and assert it was created
    as expected

    NOTE: If calling test is using the hive_unittest_metastore_db fixture, these tables will be blown away after
    each test (and the metastore_db itself will be blown away after the full test session)
    """
    delta_table_spec = TABLE_SPEC[delta_table_name]

    cmd_args = [f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}"]
    expected_db_name = delta_table_spec["destination_database"]
    if alt_db:
        cmd_args += [f"--alt-db={alt_db}"]
        expected_db_name = alt_db
    expected_table_name = delta_table_name.split(".")[-1]
    if alt_name:
        cmd_args += [f"--alt-name={alt_name}"]
        expected_table_name = alt_name
    call_command("create_delta_table", *cmd_args)

    schemas = spark.sql("show schemas").collect()
    assert expected_db_name in [s["namespace"] for s in schemas]

    # NOTE: The ``use <db>`` from table creation is still in effect for this verification. So no need to call again
    tables = spark.sql("show tables").collect()
    assert expected_table_name in [t["tableName"] for t in tables]
    delta_table_metadata_rows = [
        t for t in tables if t["namespace"] == expected_db_name and t["tableName"] == expected_table_name
    ]
    assert len(delta_table_metadata_rows) == 1
    assert delta_table_metadata_rows[0]["namespace"] == expected_db_name
    assert delta_table_metadata_rows[0]["isTemporary"] is False


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


def test_create_delta_table_for_recipient_lookup_testing(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "recipient_lookup_testing", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_profile(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "recipient_profile", s3_unittest_data_bucket)


def test_create_delta_table_for_sam_recipient(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "sam_recipient", s3_unittest_data_bucket)


def test_create_delta_table_for_summary_state_view(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "summary_state_view", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fabs(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_fabs", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_fpds(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_fpds", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_normalized(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_normalized", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_search(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_search", s3_unittest_data_bucket)


def test_create_delta_table_for_transaction_search_testing(spark, s3_unittest_data_bucket, hive_unittest_metastore_db):
    _verify_delta_table_creation(spark, "transaction_search_testing", s3_unittest_data_bucket)


def test_create_delta_table_for_recipient_lookup_with_alt_db_and_name(
    spark, s3_unittest_data_bucket, hive_unittest_metastore_db
):
    _verify_delta_table_creation(
        spark, "recipient_lookup", s3_unittest_data_bucket, alt_db="my_alt_db", alt_name="recipient_lookup_alt_name"
    )
