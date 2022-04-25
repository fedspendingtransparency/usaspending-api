"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.
"""
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
)
from usaspending_api.config import CONFIG
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row


@fixture
def spark_stopper():
    """Throw an error if coming into a test using this fixture which needs to create a
    NEW SparkContext (i.e. new JVM invocation to run Spark in a java process)
    AND, proactively cleanup any SparkContext created by this test after it completes
    """
    if SparkContext._active_spark_context:  # Singleton instance populated if there's an active SparkContext
        raise Exception("Test needs to create a new SparkSession+SparkContext, but a SparkContext exists.")

    # Add SPARK_TEST to avoid the Spark UI starting during tests
    with patch.dict(os.environ, {"SPARK_TESTING": "true"}):
        yield
    if SparkContext._active_spark_context:
        try:
            SparkContext.getOrCreate().stop()
        except Exception:
            # Swallow errors if not able to stop (e.g. may have already been stopped)
            pass


def test_spark_app_run_local_master(request, spark_stopper):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """
    extra_conf = {}
    spark = configure_spark_session(
        app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=True, **extra_conf
    )  # type: SparkSession

    logger = get_jvm_logger(spark)

    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    logger.info(versions)


def test_spark_app_run_remote_master(request, spark_stopper):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    """
    extra_conf = {
        "spark.master": "spark://localhost:7077",
    }
    spark = configure_spark_session(
        app_name=request.node.name, log_level=logging.INFO, log_spark_config_vals=True, **extra_conf
    )  # type: SparkSession

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
@patch.dict(
    os.environ,
    {
        "PYSPARK_SUBMIT_ARGS": "--packages org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31 pyspark-shell"
    },
)
def test_spark_write_csv_app_run(request, spark_stopper):
    """More involved integration test that requires MinIO to be up as an s3 alternative."""
    extra_conf = {
        # "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31",
    }
    spark = configure_spark_session(app_name=request.node.name, **extra_conf)  # type: SparkSession

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
    df.write.option("header", True).csv(f"s3a://{CONFIG.AWS_S3_BUCKET}/{CONFIG.AWS_S3_OUTPUT_PATH}/write_to_s3/")
