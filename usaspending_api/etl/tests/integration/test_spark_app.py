"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.

NOTE: Uses Pytest Fixtures from immediate parent conftest.py: usaspending_api/etl/tests/conftest.py
"""

import logging
import random
import sys
import uuid
from datetime import date
from unittest.mock import MagicMock, call

import boto3
from django.conf import settings
from model_bakery import baker
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pytest import fixture, mark
from usaspending_api.awards.models import TransactionFABS, TransactionFPDS
from usaspending_api.common.helpers.spark_helpers import (
    get_jdbc_url_from_pg_uri,
    get_jdbc_connection_properties,
    get_broker_jdbc_url,
)
from usaspending_api.common.etl.spark import _USAS_RDS_REF_TABLES, _BROKER_REF_TABLES, create_ref_temp_views
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.config import CONFIG

logger = logging.getLogger(__name__)


def test_jvm_sparksession(spark: SparkSession):
    with SparkContext._lock:
        # Check the Singleton instance populated if there's an active SparkContext
        assert SparkContext._active_spark_context is not None
        sc = SparkContext._active_spark_context
        assert sc._jvm
        assert sc._jvm.SparkSession
        assert not sc._jvm.SparkSession.getDefaultSession().get().sparkContext().isStopped()


def test_hive_metastore_db(spark: SparkSession, s3_unittest_data_bucket, hive_unittest_metastore_db):
    """Ensure that schemas and tables created are tracked in the hive metastore_db"""
    test_schema = "my_delta_test_schema"
    test_table = "my_delta_test_table"
    spark.sql(f"create schema if not exists {test_schema}")
    spark.sql(
        f"""
        create table if not exists {test_schema}.{test_table}(id INT, name STRING, age INT)
        using delta
        location 's3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{test_table}'
    """
    )

    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]
    assert len(schemas_in_metastore) == 2
    assert "default" in schemas_in_metastore
    assert test_schema in schemas_in_metastore

    spark.sql(f"USE {test_schema}")
    tables_in_test_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    assert len(tables_in_test_schema) == 1
    assert tables_in_test_schema[0]["namespace"] == test_schema
    assert tables_in_test_schema[0]["tableName"] == test_table


def test_tmp_hive_metastore_db_empty_on_test_start(spark: SparkSession, hive_unittest_metastore_db):
    """Test that when using the spark test fixture, the metastore_db is configured to live in a tmp directory,
    so that schemas and tables created while under-test only live or are known for the duration of a SINGLE test,
    not a test SESSION. And test that the metastore used for unit tests is empty on each test run (except for the
    empty "default" database"""
    # Ensure only the default schema exists
    schemas_in_metastore = [s[0] for s in spark.sql("SHOW SCHEMAS").collect()]
    assert len(schemas_in_metastore) == 1
    assert schemas_in_metastore[0] == "default"

    # Ensure the default schema has no tables
    spark.sql("USE DEFAULT")
    tables_in_default_schema = [t for t in spark.sql("SHOW TABLES").collect()]
    assert len(tables_in_default_schema) == 0


def test_spark_app_run_local_master(spark: SparkSession):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """

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
    agency1 = baker.make("references.Agency", _fill_optional=True)
    awd1 = baker.make("search.AwardSearch", award_id=1, awarding_agency_id=agency1.id)
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award=awd1,
        modification_number="1",
        awarding_agency_id=agency1.id,
        last_modified_date=date(2012, 3, 1),
        business_funds_indicator="a",
        record_type=1,
        total_funding_amount=1000.00,
        is_fpds=False,
    )
    assert TransactionFABS.objects.all().count() == 1

    awd2 = baker.make("search.AwardSearch", award_id=1, awarding_agency_id=agency1.id)
    baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        award=awd2,
        modification_number="1",
        awarding_agency_id=agency1.id,
        last_modified_date=date(2012, 4, 1),
        is_fpds=True,
        piid="abc",
        base_and_all_options_value=1000,
    )
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
    pg_uri = get_database_dsn_string()
    jdbc_url = get_jdbc_url_from_pg_uri(pg_uri)
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

    schema_name = delta_lake_unittest_schema

    # ==== transaction_normalized ====
    table_name = "vw_transaction_normalized"
    logger.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=get_jdbc_connection_properties())
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    logger.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{table_name}",
        mode="overwrite",
        path=path,
    )

    # ==== transaction_fabs ====
    table_name = "vw_transaction_fabs"
    logger.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=get_jdbc_connection_properties())
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    logger.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

    # Create table in the metastore using DataFrame's schema and write data to the table
    df.write.saveAsTable(
        format="delta",
        name=f"{table_name}",
        mode="overwrite",
        path=path,
    )

    # ==== transaction_fpds ====
    table_name = "vw_transaction_fpds"
    logger.info(f"Reading db records for {table_name} from connection: {jdbc_url}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=get_jdbc_connection_properties())
    # NOTE! NOTE! NOTE! MinIO locally does not support a TRAILING SLASH after object (folder) name
    path = f"s3a://{s3_unittest_data_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{table_name}"

    logger.info(f"Loading {df.count()} rows from DB to Delta table named {schema_name}.{table_name} at path {path}")

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
    assert "vw_transaction_normalized" in schema_table_names
    assert "vw_transaction_fabs" in schema_table_names
    assert "vw_transaction_fpds" in schema_table_names

    # Now assert that we're still by-default using the unittest schema, by way of using that pytest fixture.
    # i.e. don't tell it what schema to look at
    tables = spark.sql(f"show tables").collect()
    assert len(tables) == 3
    table_names = [t.tableName for t in tables]
    assert "vw_transaction_normalized" in table_names
    assert "vw_transaction_fabs" in table_names
    assert "vw_transaction_fpds" in table_names

    # Assert rows are present
    assert spark.sql("select count(*) from vw_transaction_normalized").collect()[0][0] == 2
    assert spark.sql("select count(*) from vw_transaction_fabs").collect()[0][0] == 1
    assert spark.sql("select count(*) from vw_transaction_fpds").collect()[0][0] == 1


@mark.skipif(
    settings.DATA_BROKER_DB_ALIAS not in settings.DATABASES,
    reason="'data_broker' database not configured in django settings.DATABASES.",
)
@mark.django_db(transaction=True)
def test_create_ref_temp_views(spark: SparkSession):
    # Add dummy data to each test views
    for rds_ref_table in _USAS_RDS_REF_TABLES:
        baker.make(rds_ref_table)
        baker.make(rds_ref_table)
        baker.make(rds_ref_table)

    # make the temp views
    create_ref_temp_views(spark)

    # verify the data in the temp view matches the dummy data
    for rds_ref_table in _USAS_RDS_REF_TABLES:
        spark_count = spark.sql(f"select count(*) from global_temp.{rds_ref_table._meta.db_table}").collect()[0][0]
        assert rds_ref_table.objects.count() == spark_count

    # Setup for testing the Broker table(s)
    mock_spark_session = MagicMock(autospec=SparkSession)
    mock_broker_sql_strings = []
    jdbc_conn_props = get_jdbc_connection_properties()

    for broker_table in _BROKER_REF_TABLES:
        mock_broker_sql_strings.append(
            call(
                f"""
            CREATE OR REPLACE GLOBAL TEMPORARY VIEW {broker_table}
            USING JDBC
            OPTIONS (
                driver '{jdbc_conn_props["driver"]}',
                fetchsize '{jdbc_conn_props["fetchsize"]}',
                url '{get_broker_jdbc_url()}',
                dbtable '{broker_table}'
            )
            """
            )
        )

    # Test that the Broker's SQL is being run when create_broker_views=True
    create_ref_temp_views(mock_spark_session, create_broker_views=True)
    mock_spark_session.sql.assert_called()
    mock_spark_session.sql.assert_has_calls(mock_broker_sql_strings, any_order=True)

    # Test that the Broker's SQL is NOT being called when create_broker_views=False
    mock_spark_session.sql.reset_mock()
    create_ref_temp_views(mock_spark_session)
    for broker_mock_call in mock_broker_sql_strings:
        assert broker_mock_call not in mock_spark_session.sql.mock_calls
