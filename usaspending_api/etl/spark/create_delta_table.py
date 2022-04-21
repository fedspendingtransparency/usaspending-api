import os
import sys

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_jvm_logger
from usaspending_api.transactions.models.spark.source_assistance_transaction import (
    source_assististance_transaction_sql_string,
)
from pyspark.sql import SparkSession

SQL_MAPPING = {"source_assistance_transaction": source_assististance_transaction_sql_string}


def main():
    extra_conf = {
        # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # See comment below about old date and time values cannot parsed without these
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
    }
    spark = configure_spark_session(**extra_conf)  # type: SparkSession

    # Setup Logger
    logger = get_jvm_logger(spark)
    logger.info("PySpark Job started!")
    logger.info(
        f"""
    @       Python Version: {sys.version}
    @       Spark Version: {spark.version}
    @       Hadoop Version: {spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
    """
    )

    # Resolve Parameters
    DESTINATION_SCHEMA = os.environ.get("DESTINATION_SCHEMA", "bronze")
    DESTINATION_TABLE = os.environ.get("DESTINATION_TABLE")

    # Setup DB Schema
    if DESTINATION_SCHEMA:
        # Set the database that will be interacted with for all Delta Lake table Spark-based activity from here forward
        logger.info(f"Using Spark Database: {DESTINATION_SCHEMA}")
        spark.sql(f"create database if not exists {DESTINATION_SCHEMA};")
        spark.sql(f"use {DESTINATION_SCHEMA};")

    # Define Schema Using CREATE TABLE AS command
    spark.sql(
        SQL_MAPPING[DESTINATION_TABLE].format(
            DESTINATION_SCHEMA=DESTINATION_SCHEMA,
            DESTINATION_TABLE=DESTINATION_TABLE,
            AWS_S3_BUCKET=CONFIG.AWS_S3_BUCKET,
            AWS_S3_OUTPUT_PATH=CONFIG.AWS_S3_OUTPUT_PATH,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
