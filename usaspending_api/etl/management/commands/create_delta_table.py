from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.config import CONFIG
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_jvm_logger,
    get_active_spark_session,
)
from usaspending_api.awards.delta_models import awards_sql_string, financial_accounts_by_awards_sql_string
from usaspending_api.recipient.delta_models import (
    recipient_lookup_sql_string,
    recipient_profile_sql_string,
    sam_recipient_sql_string,
)
from usaspending_api.transactions.delta_models import (
    transaction_fabs_sql_string,
    transaction_fpds_sql_string,
    transaction_normalized_sql_string,
    transaction_search_sql_string,
)
from usaspending_api.search.delta_models.award_search import award_search_sql_string

from usaspending_api.recipient.models import DUNS, RecipientLookup, RecipientProfile
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    TransactionFABS,
    TransactionFPDS,
    TransactionNormalized,
)


TABLE_SPEC = {
    "award_search": {
        "model": None,
        "source_table": None,
        "destination_database": "rpt",
        "partition_column": None,
        "partition_column_type": None,
        "delta_table_create_sql": award_search_sql_string,
        "custom_schema": None,
    },
    "awards": {
        "model": Award,
        "source_table": "awards",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": awards_sql_string,
        "custom_schema": "",
    },
    "financial_accounts_by_awards": {
        "model": FinancialAccountsByAwards,
        "source_table": "financial_accounts_by_awards",
        "destination_database": "raw",
        "partition_column": "financial_accounts_by_awards_id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": financial_accounts_by_awards_sql_string,
        "custom_schema": "",
    },
    "recipient_lookup": {
        "model": RecipientLookup,
        "source_table": "recipient_lookup",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": recipient_lookup_sql_string,
        "custom_schema": "recipient_hash STRING",
    },
    "recipient_profile": {
        "model": RecipientProfile,
        "source_table": "recipient_profile",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": recipient_profile_sql_string,
        "custom_schema": "recipient_hash STRING",
    },
    "sam_recipient": {
        "model": DUNS,
        "source_table": "duns",
        "destination_database": "raw",
        "partition_column": None,
        "partition_column_type": None,
        "delta_table_create_sql": sam_recipient_sql_string,
        "custom_schema": "broker_duns_id INT, business_types_codes ARRAY<STRING>",
    },
    "transaction_fabs": {
        "model": TransactionFABS,
        "source_table": "transaction_fabs",
        "destination_database": "raw",
        "partition_column": "published_fabs_id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": transaction_fabs_sql_string,
        "custom_schema": "",
    },
    "transaction_fpds": {
        "model": TransactionFPDS,
        "source_table": "transaction_fpds",
        "destination_database": "raw",
        "partition_column": "detached_award_procurement_id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": transaction_fpds_sql_string,
        "custom_schema": "",
    },
    "transaction_normalized": {
        "model": TransactionNormalized,
        "source_table": "transaction_normalized",
        "destination_database": "raw",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": transaction_normalized_sql_string,
        "custom_schema": "",
    },
    "transaction_search": {
        "model": None,  # Placeholder for now
        "source_table": None,  # Placeholder for now
        "destination_database": "rpt",
        "partition_column": None,  # Placeholder for now
        "partition_column_type": None,  # Placeholder for now
        "delta_table_create_sql": transaction_search_sql_string,
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
        parser.add_argument(
            "--alt-db",
            type=str,
            required=False,
            help="An alternate database (aka schema) in which to create this table, overriding the TABLE_SPEC db",
        )
        parser.add_argument(
            "--alt-name",
            type=str,
            required=False,
            help="An alternate delta table name for the created table, overriding the TABLE_SPEC destination_table "
            "name",
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
        destination_database = options["alt_db"] or table_spec["destination_database"]
        destination_table_name = options["alt_name"] or destination_table

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"create database if not exists {destination_database};")
        spark.sql(f"use {destination_database};")

        spark.sql(f"DROP TABLE IF EXISTS {destination_table_name}")

        # Define Schema Using CREATE TABLE AS command
        spark.sql(
            TABLE_SPEC[destination_table]["delta_table_create_sql"].format(
                DESTINATION_TABLE=destination_table_name,
                DESTINATION_DATABASE=destination_database,
                SPARK_S3_BUCKET=spark_s3_bucket,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
            )
        )

        if spark_created_by_command:
            spark.stop()
