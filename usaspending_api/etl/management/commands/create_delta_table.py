from django.core.management.base import BaseCommand

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_jvm_logger,
    get_active_spark_session,
)
from usaspending_api.recipient.delta_models.sam_recipient import sam_recipient_sql_string
from usaspending_api.transactions.delta_models.transaction_fabs import transaction_fabs_sql_string
from usaspending_api.transactions.delta_models.transaction_fpds import transaction_fpds_sql_string
from usaspending_api.recipient.delta_models.recipient_lookup import recipient_lookup_sql_string

from pyspark.sql import SparkSession

TABLE_SPEC = {
    "recipient_lookup": {
        "schema_sql_string": recipient_lookup_sql_string,
        "source_table": "recipient_lookup",
        "source_database": "",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
    },
    "sam_recipient": {
        "schema_sql_string": sam_recipient_sql_string,
        "source_table": "duns",
        "source_database": "",
        "destination_database": "raw",
        "partition_column": "update_date",
        "partition_column_type": "date",
    },
    "transaction_fabs": {
        "schema_sql_string": transaction_fabs_sql_string,
        "source_table": "transaction_fabs",
        "source_database": "",
        "destination_database": "raw",
        "partition_column": "published_fabs_id",
        "partition_column_type": "numeric",
    },
    "transaction_fpds": {
        "schema_sql_string": transaction_fpds_sql_string,
        "source_table": "transaction_fpds",
        "source_database": "",
        "destination_database": "raw",
        "partition_column": "detached_award_procurement_id",
        "partition_column_type": "numeric",
    },
}


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
        }

        spark = get_active_spark_session()
        if not spark:
            spark = configure_spark_session(**extra_conf, spark_context=spark)  # type: SparkSession

        # Setup Logger
        logger = get_jvm_logger(spark)

        # Resolve Parameters
        destination_table = options["destination_table"]

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
                AWS_S3_BUCKET=CONFIG.AWS_S3_BUCKET,
                AWS_S3_OUTPUT_PATH=CONFIG.AWS_S3_OUTPUT_PATH,
            )
        )

        spark.stop()
