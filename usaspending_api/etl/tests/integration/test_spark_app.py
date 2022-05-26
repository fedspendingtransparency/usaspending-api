"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.
"""
import logging
import random
import sys
import uuid
import tempfile
import shutil
import os
from datetime import date

import boto3
from model_mommy import mommy
from pyspark.sql import SparkSession, Row
from pytest import fixture, mark
from usaspending_api.awards.models import TransactionFABS, TransactionFPDS
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
from usaspending_api.common.helpers.spark_helpers import (
    get_jvm_logger,
    is_spark_context_stopped,
    stop_spark_context,
    get_jdbc_url_from_pg_uri,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.config import CONFIG

# How to determine a working dependency set:
# 1. What platform are you using? local dev with pip-installed PySpark? EMR 6.x or 5.x? Databricks Runtime?
# 2. From there determine what versions of Spark + Hadoop are supported on that platform. If going cross-platform,
#    try to pick a combo that's supporpted on both
# 3. Is there a hadoop-aws version matching the platform's Hadoop version used? Because we need to have Spark writing
#    to S3, we are beholden to the AWS-provided JARs that implement the S3AFileSystem, which are part of the
#    hadoop-aws JAR.
# 4. Going from the platform-hadoop version, find the same version of hadoop-aws up in
#    https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/
#    and look to see what version its dependent JARs are at that your code requires are runtime. If seeing errors or are
#    uncertain of compatibility, see what working version-sets are aligned to an Amazon EMR release here:
#    https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html
_SCALA_VERSION = "2.12"
_HADOOP_VERSION = "3.2.0"
_SPARK_VERSION = "3.1.2"
_DELTA_VERSION = "1.0.0"

# List of Maven coordinates for required JAR files used by running code, which can be added to the driver and
# executor class paths
SPARK_SESSION_JARS = [
    # "com.amazonaws:aws-java-sdk:1.12.31",
    # hadoop-aws is an add-on to hadoop with Classes that allow hadoop to interface with an S3A (AWS S3) FileSystem
    # NOTE That in order to work, the version number should be the same as the Hadoop version used by your Spark runtime
    # It SHOULD pull in (via Ivy package manager from maven repo) the version of com.amazonaws:aws-java-sdk that is
    # COMPATIBLE with it (so that should not  be set as a dependent package by us)
    f"org.apache.hadoop:hadoop-aws:{_HADOOP_VERSION}",
    "org.postgresql:postgresql:42.2.23",
    f"io.delta:delta-core_{_SCALA_VERSION}:{_DELTA_VERSION}",
]

DELTA_LAKE_UNITTEST_SCHEMA_NAME = "unittest"


@fixture(scope="session")
def spark_data_output_folder():
    """Create a test directory to be used for the spark tests """
    prefix = "unittest-tmpdir-"
    tmp_dir_path = tempfile.mkdtemp(prefix=prefix)
    logging.info(f"Unit Test Directory '{tmp_dir_path}' created.")

    yield tmp_dir_path

    # Cleanup by removing all objects in the temp directory
    shutil.rmtree(tmp_dir_path)
    logging.info(f"Unit Test Directory '{tmp_dir_path}' removed.")


@fixture(scope="session")
def s3_unittest_data_bucket_setup_and_teardown():
    """Create a test bucket named data so the tests can use it"""
    unittest_data_bucket = "unittest-data-{}".format(str(uuid.uuid4()))

    logging.warning(
        f"Attempting to create unit test data bucket {unittest_data_bucket } "
        f"at: http://{CONFIG.SPARK_S3_BUCKET} using CONFIG.AWS_ACCESS_KEY and CONFIG.AWS_SECRET_KEY"
    )
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    from botocore.errorfactory import ClientError

    try:
        s3_client.create_bucket(Bucket=unittest_data_bucket)
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            # Simplest way to ensure the bucket is created is to swallow the exception saying it already exists
            logging.warning("Unit Test Data Bucket not created; already exists.")
            pass
        else:
            raise e

    logging.info(
        f"Unit Test Data Bucket '{unittest_data_bucket}' created (or found to exist) at S3 endpoint "
        f"'{unittest_data_bucket}'. Current Buckets:"
    )
    [logging.info(f"  {b['Name']}") for b in s3_client.list_buckets()["Buckets"]]

    yield unittest_data_bucket

    # Cleanup by removing all objects in the bucket by key, and then the bucket itsefl after the test session
    response = s3_client.list_objects_v2(Bucket=unittest_data_bucket)
    if "Contents" in response:
        for object in response["Contents"]:
            s3_client.delete_object(Bucket=unittest_data_bucket, Key=object["Key"])
    s3_client.delete_bucket(Bucket=unittest_data_bucket)


@fixture
def s3_unittest_data_bucket(s3_unittest_data_bucket_setup_and_teardown):
    """Use the S3 unit test data bucket created for the test session, and cleanup any contents created in it after
    each test
    """
    unittest_data_bucket = s3_unittest_data_bucket_setup_and_teardown
    yield unittest_data_bucket

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    # Cleanup any contents added to the bucket for this test
    response = s3_client.list_objects_v2(Bucket=unittest_data_bucket)
    if "Contents" in response:
        for object in response["Contents"]:
            s3_client.delete_object(Bucket=unittest_data_bucket, Key=object["Key"])
    # NOTE: Leave the bucket itself there for other tests in this session. It will get cleaned up at the end of the
    # test session by the dependent fixture


@fixture(scope="session")
def spark() -> SparkSession:
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes

    This fixture will create ONE single SparkContext to be shared by ALL unit tests (and therefore must be populated
    with universally compatible config and with the superset of all JAR dependencies our test code might need.
    """
    if not is_spark_context_stopped():
        raise Exception(
            "Error: Test session cannot create a SparkSession because one already exists at the time this "
            "test-session-scoped fixture is being evaluated."
        )

    extra_conf = {
        # This is the default, but being explicit
        "spark.master": "local[*]",
        # Client deploy mode is the default, but being explicit.
        # Means the driver node is the place where the SparkSession is instantiated (and/or where spark-submit
        # process is started from, even if started under the hood of a Py4J JavaGateway). With a "standalone" (not
        # YARN or Mesos or Kubernetes) cluster manager, only client mode is supported.
        "spark.submit.deployMode": "client",
        "spark.ui.enabled": "false",  # Does the same as setting SPARK_TESTING=true env var
        "spark.jars.packages": ",".join(SPARK_SESSION_JARS),
        # Delta Lake config for Delta tables and SQL. Need these to keep Delta table metadata in the metastore
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # See comment below about old date and time values cannot parsed without these
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
    }
    spark = configure_spark_session(
        app_name="Unit Test Session",
        log_level=logging.INFO,
        log_spark_config_vals=True,
        **extra_conf,
    )  # type: SparkSession

    yield spark

    stop_spark_context()


@fixture
def delta_lake_unittest_schema(spark: SparkSession):
    """Specify which Delta 'SCHEMA' to use (NOTE: 'SCHEMA' and 'DATABASE' are interchangeable in Delta Spark SQL),
    and cleanup any objects created in the schema after the test run."""

    # Force default usage of the unittest schema in this SparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")
    spark.sql(f"USE {DELTA_LAKE_UNITTEST_SCHEMA_NAME}")

    # Yield the name of the db that test delta lake tables and records should be put in.
    yield DELTA_LAKE_UNITTEST_SCHEMA_NAME

    # Cascade will remove the tables and functions in this SCHEMA
    spark.sql(f"DROP SCHEMA IF EXISTS {DELTA_LAKE_UNITTEST_SCHEMA_NAME} CASCADE")


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


def test_spark_write_csv_app_run(spark: SparkSession, s3_unittest_data_bucket, spark_data_output_folder):
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
    csv_path = f"s3a://{s3_unittest_data_bucket}" f"/{CONFIG.DELTA_LAKE_S3_PATH}/write_to_s3"
    if CONFIG.USE_FILESYSTEM_FOR_SPARK_DATA:
        csv_path = f"file://{spark_data_output_folder}/write_to_s3"
    df.write.option("header", True).csv(csv_path)

    # Verify there are *.csv part files in the chosen bucket
    if not CONFIG.USE_FILESYSTEM_FOR_SPARK_DATA:
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
    else:
        bucket_objects = os.listdir(f"{spark_data_output_folder}/write_to_s3")
        assert len(bucket_objects) > 0
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
    spark_data_output_folder,
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
    if CONFIG.USE_FILESYSTEM_FOR_SPARK_DATA:
        path = f"file://{spark_data_output_folder}/{table_name}"

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
    if CONFIG.USE_FILESYSTEM_FOR_SPARK_DATA:
        path = f"file://{spark_data_output_folder}/{table_name}"

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
    if CONFIG.USE_FILESYSTEM_FOR_SPARK_DATA:
        path = f"file://{spark_data_output_folder}/{table_name}"

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
