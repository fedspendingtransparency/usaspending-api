"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""
import logging
import random
import sys
import uuid
from datetime import date

import boto3
from model_mommy import mommy
from pyspark.sql import SparkSession, Row
from pytest import fixture, mark
from usaspending_api.awards.models import TransactionFABS, TransactionFPDS
from usaspending_api.common.helpers.spark_helpers import (
    get_jvm_logger,
    get_jdbc_url_from_pg_uri,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.config import CONFIG


def test_spark_app_run_local_master(spark: SparkSession):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """
    logger = get_jvm_logger(spark)

    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    logger.info(versions)


def test_spark_write_csv_app_run(spark: SparkSession, s3_unittest_data_bucket):
    """More involved integration test that requires MinIO to be up as an s3 alternative."""
    data = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    df = spark.createDataFrame([Row(**data_row) for data_row in data])
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    df.write.option("header", True).csv(f"s3a://{s3_unittest_data_bucket}" f"/{CONFIG.DELTA_LAKE_S3_PATH}/write_to_s3")

    # Verify there are *.csv part files in the chosen bucket
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )
    response = s3_client.list_objects_v2(Bucket=s3_unittest_data_bucket)
    assert "Contents" in response  # the Bucket has contents
    bucket_objects = [c["Key"] for c in response["Contents"]]
    assert any([obj.endswith(".csv") for obj in bucket_objects])


@fixture()
def _transaction_and_award_test_data(db):
    agency1 = mommy.make("references.Agency")
    awd1 = mommy.make("awards.Award", awarding_agency=agency1)
    txn1 = mommy.make(
        "awards.TransactionNormalized",
        award=awd1,
        modification_number="1",
        awarding_agency=agency1,
        last_modified_date=date(2012, 3, 1),
    )
    mommy.make(
        "awards.TransactionFABS",
        transaction=txn1,
        business_funds_indicator="a",
        record_type=1,
        total_funding_amount=1000.00,
    )
    assert TransactionFABS.objects.all().count() == 1

    awd2 = mommy.make("awards.Award", awarding_agency=agency1)
    txn2 = mommy.make(
        "awards.TransactionNormalized",
        award=awd2,
        modification_number="1",
        awarding_agency=agency1,
        last_modified_date=date(2012, 4, 1),
    )
    mommy.make("awards.TransactionFPDS", transaction=txn2, piid="abc", base_and_all_options_value=1000)
    assert TransactionFPDS.objects.all().count() == 1


@mark.django_db(transaction=True)  # must commit Django data for Spark to be able to read it
def test_spark_write_to_s3_delta_from_db(
    _transaction_and_award_test_data,
    spark: SparkSession,
    delta_lake_unittest_schema,
    s3_unittest_data_bucket,
    request,
):
    """Test that we can read from Postgres DB and write to new delta tables,
    and the tables are created and data gets there"""
    jdbc_conn_props = {"driver": "org.postgresql.Driver", "fetchsize": str(CONFIG.SPARK_PARTITION_ROWS)}

    pg_uri = get_database_dsn_string()
    jdbc_url = get_jdbc_url_from_pg_uri(pg_uri)
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

    schema_name = delta_lake_unittest_schema

    # ==== transaction_normalized ====
    table_name = "transaction_normalized"
    logging.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_conn_props)
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    log = get_jvm_logger(spark, request.node.name)
    log.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{table_name}",
        mode="overwrite",
        path=path,
    )

    # ==== transaction_fabs ====
    table_name = "transaction_fabs"
    logging.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_conn_props)
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    log = get_jvm_logger(spark, request.node.name)
    log.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{table_name}",
        mode="overwrite",
        path=path,
    )

    # ==== transaction_fpds ====
    table_name = "transaction_fpds"
    logging.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_conn_props)
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    log = get_jvm_logger(spark, request.node.name)
    log.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{table_name}",
        mode="overwrite",
        path=path,
    )

    schema_tables = spark.sql(f"show tables in {schema_name}").collect()
    assert len(schema_tables) == 3
    schema_table_names = [t.tableName for t in schema_tables]
    assert "transaction_normalized" in schema_table_names
    assert "transaction_fabs" in schema_table_names
    assert "transaction_fpds" in schema_table_names

    # Now assert that we're still by-default using the unittest schema, by way of using that pytest fixture.
    # i.e. don't tell it what schema to look at
    tables = spark.sql(f"show tables").collect()
    assert len(tables) == 3
    table_names = [t.tableName for t in tables]
    assert "transaction_normalized" in table_names
    assert "transaction_fabs" in table_names
    assert "transaction_fpds" in table_names

    # Assert rows are present
    assert spark.sql("select count(*) from transaction_normalized").collect()[0][0] == 2
    assert spark.sql("select count(*) from transaction_fabs").collect()[0][0] == 1
    assert spark.sql("select count(*) from transaction_fpds").collect()[0][0] == 1
