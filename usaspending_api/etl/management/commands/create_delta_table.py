import sys
import time

from django.core.management.base import BaseCommand

from usaspending_api.awards.delta_models.broker_subaward import broker_subaward_sql_string
from usaspending_api.awards.delta_models.financial_accounts_by_awards import financial_accounts_by_awards_sql_string
from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_jvm_logger
from usaspending_api.recipient.delta_models.sam_recipient import sam_recipient_sql_string
from usaspending_api.transactions.delta_models.source_assistance_transaction import (
    source_assististance_transaction_sql_string,
)
from usaspending_api.transactions.delta_models.source_procurement_transaction import (
    source_procurement_transaction_sql_string,
)
from pyspark.sql import SparkSession

TABLE_SPEC = {
    "broker_subaward": {
        "schema_sql_string": broker_subaward_sql_string,
        "source_table": "subaward",
        "source_database": "",
        "external_load_date_key": "broker_subaward_delta",
        "partition_column": "id",
        "partition_column_type": "",
        "merge_column": "id",
        "last_update_column": "updated_at",
        "force_full_reload": False
    },
    "financial_accounts_by_awards": {
        "schema_sql_string": financial_accounts_by_awards_sql_string,
        "source_table": "financial_accounts_by_awards",
        "source_database": "",
        "external_load_date_key": "financial_accounts_by_awards_delta",
        "partition_column": "reporting_period_end",
        "partition_column_type": "",
        "merge_column": "financial_accounts_by_awards_id",
        "last_update_column": "update_date",
        "force_full_reload": False
    },
    "sam_recipient": {
        "schema_sql_string": sam_recipient_sql_string,
        "source_table": "sam_recipient",
        "source_database": "",
        "external_load_date_key": None,
        "partition_column": "sam_recipient_id",
        "partition_column_type": "",
        "merge_column": None,
        "last_update_column": None,
        "force_full_reload": True 
    },
    "source_assistance_transaction": {
        "schema_sql_string": source_assististance_transaction_sql_string,
        "source_table": "published_award_financial_assistance",
        "source_database": "",
        "external_load_date_key": "source_assistance_transaction_delta",
        "partition_column": "published_award_financial_assistance_id",
        "partition_column_type": "",
        "merge_column": "afa_generated_unique",
        "last_update_column": "updated_at",
        "force_full_reload": False
    },
    "source_procurement_transaction": {
        "schema_sql_string": source_procurement_transaction_sql_string,
        "source_table": "detached_award_procurement",
        "source_database": "",
        "external_load_date_key": "source_procurement_transaction_delta",
        "partition_column": "detatched_award_procurement_id",
        "partition_column_type": "",
        "merge_column": "detatched_award_procurement_id",
        "last_update_column": "updated_at",
        "force_full_reload": False
    },
}

# TODO - Read this from Config
# TODO - Rename to more formal names (RAW/INTERMEDIATE/REPORTING)
DESTINATION_SCHEMA = "bronze"


class Command(BaseCommand):

    help = """

    """

    def add_arguments(self, parser):
        parser.add_argument("--destination-table", type=str, required=True, help="", choices=list(TABLE_SPEC.keys()))

    def handle(self, *args, **options):
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.warehouse.dir": "/project/warehouse_dir"
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
            TABLE_SPEC[destination_table]["schema_sql_string"].format(
                DESTINATION_TABLE=destination_table,
                AWS_S3_BUCKET=CONFIG.AWS_S3_BUCKET,
                AWS_S3_OUTPUT_PATH=CONFIG.AWS_S3_OUTPUT_PATH,
            )
        )

        # TODO - Write out updated external load date

        spark.stop()
