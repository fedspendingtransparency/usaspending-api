from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_jvm_logger,
    get_active_spark_session,
)
from usaspending_api.recipient.delta_models import (
    recipient_lookup_sql_string,
    recipient_profile_sql_string,
    sam_recipient_sql_string,
)
from usaspending_api.transactions.delta_models import (
    transaction_fabs_sql_string,
    transaction_fpds_sql_string,
    transaction_search_sql_string,
)


TABLE_SPEC = {
    "recipient_lookup": {
        "schema_sql_string": recipient_lookup_sql_string,
        "source_table": "recipient_lookup",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "custom_schema": "",
    },
    "recipient_profile": {
        "schema_sql_string": recipient_profile_sql_string,
        "source_table": "recipient_profile",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "custom_schema": "",
    },
    "sam_recipient": {
        "schema_sql_string": sam_recipient_sql_string,
        "source_table": "duns",
        "destination_database": "raw",
        "partition_column": "broker_duns_id",
        "partition_column_type": "numeric",
        "custom_schema": "broker_duns_id INT, business_types_codes ARRAY<STRING>",
    },
    "transaction_fabs": {
        "schema_sql_string": transaction_fabs_sql_string,
        "source_table": "transaction_fabs",
        "destination_database": "raw",
        "partition_column": "published_fabs_id",
        "partition_column_type": "numeric",
        "custom_schema": "",
    },
    "transaction_fpds": {
        "schema_sql_string": transaction_fpds_sql_string,
        "source_table": "transaction_fpds",
        "destination_database": "raw",
        "partition_column": "detached_award_procurement_id",
        "partition_column_type": "numeric",
        "custom_schema": "",
    },
    "transaction_search": {
        "schema_sql_string": transaction_search_sql_string,
        "source_table": None,  # Placeholder for now
        "destination_database": "rpt",
        "partition_column": None,  # Placeholder for now
        "partition_column_type": None,  # Placeholder for now
        "custom_schema": None,  # Placeholder for now
    },
}


class Command(BaseCommand):

    help = """
    This command creates an empty Delta Table based on the provided --destination-table argument.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--destination-table",
            type=str,
            required=True,
            help="The destination Delta Table to write the data",
            choices=list(TABLE_SPEC.keys()),
        )
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 to write the data",
        )

    def handle(self, *args, **options):
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
        }

        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)  # type: SparkSession

        # Setup Logger
        logger = get_jvm_logger(spark)

        # Resolve Parameters
        destination_table = options["destination_table"]
        spark_s3_bucket = options["spark_s3_bucket"]

        table_spec = TABLE_SPEC[destination_table]
        destination_database = table_spec["destination_database"]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"create database if not exists {destination_database};")
        spark.sql(f"use {destination_database};")

        spark.sql(f"DROP TABLE IF EXISTS {destination_table}")

        # Define Schema Using CREATE TABLE AS command
        spark.sql(
            TABLE_SPEC[destination_table]["schema_sql_string"].format(
                DESTINATION_TABLE=destination_table,
                DESTINATION_DATABASE=table_spec["destination_database"],
                SPARK_S3_BUCKET=spark_s3_bucket,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
            )
        )

        if spark_created_by_command:
            spark.stop()
