import logging
import uuid

import boto3
from pyspark.sql import SparkSession
from pytest import fixture
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
from usaspending_api.common.helpers.spark_helpers import (
    is_spark_context_stopped,
    stop_spark_context,
)
from usaspending_api.config import CONFIG


# ==== Spark Automated Integration Test Fixtures ==== #

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
def s3_unittest_data_bucket_setup_and_teardown():
    """Create a test bucket named data so the tests can use it"""
    unittest_data_bucket = "unittest-data-{}".format(str(uuid.uuid4()))

    logging.warning(
        f"Attempting to create unit test data bucket {unittest_data_bucket } "
        f"at: http://{CONFIG.AWS_S3_ENDPOINT} using CONFIG.AWS_ACCESS_KEY and CONFIG.AWS_SECRET_KEY"
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