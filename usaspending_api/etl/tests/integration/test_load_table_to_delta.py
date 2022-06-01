"""Automated Integration Tests for the lifecycle of Delta Lake tables

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import os
import pandas as pd

from model_bakery import baker
from pyspark.sql import SparkSession
from pytest import mark
from django.core.management import call_command
from django.db import connection, models

from usaspending_api.common.helpers.generic_helper import equal_dfs
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC
from usaspending_api.etl.management.commands.load_table_to_delta import JDBC_URL_KEY


def _verify_delta_table_loaded(spark: SparkSession, delta_table_name: str, s3_bucket: str):
    """Generic function that uses the create_delta_table command to create the given table and assert it was created
    as expected
    """
    # make the table and load it
    call_command("create_delta_table", f"--destination-table={delta_table_name}", f"--spark-s3-bucket={s3_bucket}")
    call_command("load_table_to_delta", f"--destination-table={delta_table_name}")

    # get the postgres data to compare
    model = TABLE_SPEC[delta_table_name]["model"]
    partition_col = TABLE_SPEC[delta_table_name]["partition_column"]
    dummy_data = pd.DataFrame.from_records(model.objects.order_by(partition_col).all().values())
    if TABLE_SPEC[delta_table_name]["partition_column_type"] == "numeric":
        dummy_data[partition_col] = dummy_data[partition_col].apply(int)

    # get the spark data to compare
    received_data = spark.sql(f"select * from {delta_table_name} order by {partition_col}").toPandas()
    if TABLE_SPEC[delta_table_name]["partition_column_type"] == "numeric":
        received_data[partition_col] = received_data[partition_col].apply(int)

    # compare
    assert equal_dfs(received_data, dummy_data)


@mark.django_db(transaction=True)  # must commit Django data for Spark to be able to read it
def test_load_table_to_delta_for_sam_recipient(spark, s3_unittest_data_bucket, pass_test_db_to_commands):
    baker.make("recipient.DUNS", broker_duns_id="1")
    baker.make("recipient.DUNS", broker_duns_id="2")
    _verify_delta_table_loaded(spark, "sam_recipient", s3_unittest_data_bucket)


# def test_load_table_to_delta_for_recipient_lookup(spark, s3_unittest_data_bucket):
#     _verify_delta_table_loaded(spark, "recipient_lookup", s3_unittest_data_bucket)
#
#
# def test_load_table_to_delta_for_recipient_profile(spark, s3_unittest_data_bucket):
#     _verify_delta_table_loaded(spark, "recipient_profile", s3_unittest_data_bucket)
#
#
# def test_load_table_to_delta_for_transaction_fabs(spark, s3_unittest_data_bucket):
#     _verify_delta_table_loaded(spark, "transaction_fabs", s3_unittest_data_bucket)
#
#
# def test_load_table_to_delta_for_transaction_fpds(spark, s3_unittest_data_bucket):
#     _verify_delta_table_loaded(spark, "transaction_fpds", s3_unittest_data_bucket)
