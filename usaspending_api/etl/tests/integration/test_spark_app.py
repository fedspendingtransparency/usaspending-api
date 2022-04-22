"""Module to verify that spark-based integration tests can run in our CI environment, with all required
spark components (docker-compose container services) are up and running and integratable.
"""
import logging
import sys
import uuid
import random
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, log_hadoop_config, log_spark_config, \
    get_jvm_logger
from config import CONFIG
from pyspark.sql import SparkSession, Row


def test_spark_app_run_local_master(request):
    """Execute a simple spark app and verify it logged expected output.
    Effectively if it runs without failing, it worked.

    NOTE: This will probably work regardless of whether any separately running (dockerized) spark infrastructure is
    present in the CI integration test env, because it will leverage the pyspark PyPI dependent package that is
    discovered in the PYTHONPATH, and treat the client machine as the spark driver.
    And furthermore, the default config for spark.master property if not set is local[*]
    """
    extra_conf = {}
    spark = configure_spark_session(
        app_name=request.node.name,
        log_level=logging.INFO,
        **extra_conf
    )  # type: SparkSession

    logger = get_jvm_logger(spark)
    log_spark_config(spark)
    log_hadoop_config(spark)

    versions = f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        """
    logger.info(versions)


def test_spark_app_run_remote_master(request):
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
        app_name=request.node.name,
        log_level=logging.INFO,
        **extra_conf
    )  # type: SparkSession

    logger = get_jvm_logger(spark)
    log_spark_config(spark)
    log_hadoop_config(spark)

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
def test_spark_write_csv_app_run(request):
    """More involved integration test that requires MinIO to be up as an s3 alternative."""
    extra_conf = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.31",
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
    df.write.option("header", True).csv(
        f"s3a://{CONFIG.AWS_S3_BUCKET}/{CONFIG.AWS_S3_OUTPUT_PATH}/write_to_s3/"
    )
