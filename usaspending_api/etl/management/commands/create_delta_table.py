import os
import sys

from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_jvm_logger
from usaspending_api.transactions.delta_models.source_assistance_transaction import (
    source_assististance_transaction_sql_string,
)
from pyspark.sql import SparkSession

SQL_MAPPING = {
    "source_assistance_transaction": {
        "schema_sql_string": source_assististance_transaction_sql_string,
        "broker_table": "published_award_financial_assistance",
        "external_load_date_key": "source_assistance_transaction",
        "partition_column": "published_award_financial_assistance_id",
        "merge_column": "published_award_financial_assistance_id",
        "last_update_column": "updated_at"
    }
}
DESTINATION_SCHEMA = "bronze"


class Command(BaseCommand):

    help = """

    """

    def add_arguments(self, parser):
        parser.add_argument("--destination-table", type=str, required=True, help="", choices=list(SQL_MAPPING.keys()))

    def handle(self, *args, **options):
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
        bronze_schema = CONFIG.BRONZE_SCHEMA
        destination_table = options["destination_table"]

        # Setup DB Schema
        if bronze_schema:
            # Set the database that will be interacted with for all Delta Lake table Spark-based activity
            logger.info(f"Using Spark Database: {bronze_schema}")
            spark.sql(f"create database if not exists {bronze_schema};")
            spark.sql(f"use {bronze_schema};")

        # Define Schema Using CREATE TABLE AS command
        spark.sql(
            SQL_MAPPING[destination_table]["schema_sql_string"].format(
                DESTINATION_TABLE=destination_table,
                AWS_S3_BUCKET=CONFIG.AWS_S3_BUCKET,
                AWS_S3_OUTPUT_PATH=CONFIG.AWS_S3_OUTPUT_PATH,
            )
        )

        spark.stop()
