"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.

TODO: HERE
1. see if spark.driver.host needs to be set to host.docker.internal to get back to the driver from executors
   - https://spark.apache.org/docs/latest/configuration.html#networking
2. master may need to be referred to from host: host.docker.internal so both the driver and executors can refer to it
3. MINIO_HOST needed to be set to host.docker.internal so both the driver and executors can refer to it
4. Maybe all of this is moot if we can run our containers in a minikube kubernetes cluster manager mode as per: https://spark.apache.org/docs/latest/running-on-kubernetes.html
   - would allow driver to be submitted from a pod on the cluster and run with deployMode=cluster
   - seems to pick this up (and not standalone) by way of the master URL being prefixed with k8s://
"""
import boto3
import logging
import os
import sys
import uuid
import random
from pytest import fixture
from unittest.mock import patch
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_jvm_logger,
    is_spark_context_stopped,
    stop_spark_context,
    attach_java_gateway,
    read_java_gateway_connection_info,
)
from usaspending_api.config import CONFIG
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.serializers import read_int, write_with_length, UTF8Deserializer
from py4j.java_gateway import java_import, JavaGateway, JavaObject, GatewayParameters
import os
import uuid
import random
from pyspark.java_gateway import launch_gateway
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from usaspending_api.common.helpers.spark_helpers import configure_spark_session
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
# _AWS_JAVA_SDK_VERSION = "1.11.901"

# List of Maven coordinates for required JAR files used by running code, which can be added to the driver and
# executor class paths
SPARK_SESSION_JARS = [
    # "com.amazonaws:aws-java-sdk:1.12.31",
    # hadoop-aws is an add-on to hadoop with Classes that allow hadoop to interface with an S3A (AWS S3) FileSystem
    # NOTE That in order to work, the version number should be the same as the Hadoop version used by your Spark runtime
    f"org.apache.hadoop:hadoop-aws:{_HADOOP_VERSION}",
    "org.postgresql:postgresql:42.2.23",
    f"io.delta:delta-core_{_SCALA_VERSION}:{_DELTA_VERSION}",
]


@fixture(scope="session")
def minio_test_data_bucket():
    """Create a bucket named data so the tests can use it"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}",
        aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
    )

    from botocore.errorfactory import ClientError

    try:
        s3_client.create_bucket(Bucket=CONFIG.AWS_S3_BUCKET)
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            # Simplest way to ensure the bucket is created is to swallow the exception saying it already exists
            logging.warning("Test Bucket not created, already exists.")
            pass
        else:
            raise e

    logging.info(
        f"Test Bucket '{CONFIG.AWS_S3_BUCKET}' created (or found to exist) at S3 endpoint "
        f"'{CONFIG.AWS_S3_ENDPOINT}'. Current Buckets:"
    )
    [logging.info(f"  {b['Name']}") for b in s3_client.list_buckets()["Buckets"]]

    yield


# TODO: Opting to use the other session-scoped fixture instead. Using this, can't seem to OVERRIDE the packages, or
# allow it to re-recognize the packages, and re-instatiate the JavaGateway. So the first test that runs,
# whatever pakcages are specified (or not specified) become the only set used.
# This could be because when the JavaGateway is launched, it instantiates a spark-submit process (directing it to an
# empty pyspark-shell), and it's possibly that the spark-submit process is never re-launched even after calling stop()
# on the SparkContext.
# - something to try is to manually create a NEW JavaGateway with .launch_gateway(), and then provide that as the
# value of gateway= param in creating the NEW SparkContext and see if it picks that up instead.
# - that or seeing how to aggressively kill the spark-submit process at the time of SparkContext.stop(),
# so a new process with new sparkconf/python_submit_args is instantiated with the new SparkContext
@fixture
def spark_stopper():
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes
    """
    if not is_spark_context_stopped():
        raise Exception("Test needs to create a new SparkSession+SparkContext, but a SparkContext exists.")

    # Add SPARK_TEST to avoid the Spark UI starting during tests
    with patch.dict(os.environ, {"SPARK_TESTING": "true"}):
        yield

    stop_spark_context()


@fixture(scope="session")
def spark() -> SparkSession:
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes
    """
    if not is_spark_context_stopped():
        raise Exception(
            "Error: Test session cannot create a SparkSession because one already exists at the time this "
            "test-session-scoped fixture is being evaluated."
        )

    extra_conf = {
        # "spark.master": "spark://localhost:7077",
        # Client deploy mode is the default, but being explicit.
        # Means the driver node is the place where the SparkSession is instantiated (and/or where spark-submit
        # process is started from, even if started under the hood of a Py4J JavaGateway). With a "standalone" (not
        # YARN or Mesos or Kubernetes) cluster manager, only client mode is supported.
        "spark.submit.deployMode": "client",
        #"spark.driver.host": "host.docker.internal",
        "spark.master": "spark://localhost:7077",
        "spark.ui.enabled": "false",  # Does the same as setting SPARK_TESTING=true env var
        "spark.jars.packages": ",".join(SPARK_SESSION_JARS),
    }
    spark = configure_spark_session(
        app_name="Unit Test Session",
        log_level=logging.INFO,
        log_spark_config_vals=True,
        **extra_conf,
    )  # type: SparkSession

    # Add SPARK_TEST to avoid the Spark UI starting during tests
    # with patch.dict(os.environ, {"SPARK_TESTING": "true"}):
    yield spark

    stop_spark_context()


@fixture(scope="session")
def spark_gateway() -> SparkSession:
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes
    """
    if not is_spark_context_stopped():
        raise Exception(
            "Error: Test session cannot create a SparkSession because one already exists at the time this "
            "test-session-scoped fixture is being evaluated."
        )

    extra_conf = {
        # "spark.master": "spark://spark-master:7077",
        "spark.ui.enabled": "false",  # Does the same as setting SPARK_TESTING=true env var
        "spark.jars.packages": ",".join(SPARK_SESSION_JARS),
    }

    java_gateway = attach_java_gateway(*read_java_gateway_connection_info(CONFIG._PYSPARK_DRIVER_CONN_INFO_PATH))
    spark = configure_spark_session(
        java_gateway=java_gateway,
        master="spark://spark-master:7077",
        app_name="Unit Test Session",
        log_level=logging.INFO,
        log_spark_config_vals=True,
        **extra_conf,
    )  # type: SparkSession

    # Add SPARK_TEST to avoid the Spark UI starting during tests
    # with patch.dict(os.environ, {"SPARK_TESTING": "true"}):
    yield spark

    stop_spark_context()


# def test_spark_app_run_local_master(request, spark_stopper):
def test_spark_app_run_local_master(spark: SparkSession, request):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """
    # extra_conf = {}
    # spark = configure_spark_session(
    #     app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=False, **extra_conf
    # )  # type: SparkSession

    logger = get_jvm_logger(spark)

    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    logger.info(versions)


# def test_spark_app_run_remote_master(request, spark_stopper):
def test_spark_app_run_remote_master(spark: SparkSession, request):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    """
    # extra_conf = {
    #     "spark.master": "spark://localhost:7077",
    # }
    # spark = configure_spark_session(
    #     app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=False, **extra_conf
    # )  # type: SparkSession
    logger = get_jvm_logger(spark)

    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    logger.info(versions)


# TODO: This is currently failing because there does not seem to be a way to bootstrap the spark runtime by way of
#  conf with the JAR libs it needs to pull into its java classpath. Currently exploring SparkConf values: https://spark.apache.org/docs/latest/submitting-applications.html
# TODO: Investigate setting the PYSPARK_SUBMIT_ARGS env var, which is basically everything (including --packages)
#  provided to spark-submit
# @patch.dict(
#     os.environ,
#     {
#         "PYSPARK_SUBMIT_ARGS": "--packages org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31 pyspark-shell"
#     },
# )
# def test_spark_write_csv_app_run(request, spark_stopper):
def test_spark_write_csv_app_run(spark: SparkSession, minio_test_data_bucket, request):
    """More involved integration test that requires MinIO to be up as an s3 alternative."""
    # extra_conf = {
    #     "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31",
    # }
    # spark = configure_spark_session(
    #     app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=False, **extra_conf
    # )  # type: SparkSession

    # spark = configure_spark_session(
    #     app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=False, **extra_conf
    # )  # type: SparkSession
    spark = spark.builder.appName(request.node.name).getOrCreate()

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
    df.write.option("header", True).csv(f"s3a://{CONFIG.AWS_S3_BUCKET}"f"/{CONFIG.AWS_S3_OUTPUT_PATH}/write_to_s3")
