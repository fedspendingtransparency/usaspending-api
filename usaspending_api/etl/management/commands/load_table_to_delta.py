from django.core.management import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.awards.delta_models import awards_sql_string, financial_accounts_by_awards_sql_string
from usaspending_api.common.etl.spark import extract_db_data_frame, get_partition_bounds_sql, load_delta_table
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jdbc_url,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.recipient.delta_models import (
    recipient_lookup_sql_string,
    recipient_profile_sql_string,
    sam_recipient_sql_string,
)
from usaspending_api.search.models import TransactionSearch, AwardSearchView
from usaspending_api.transactions.delta_models import (
    transaction_fabs_sql_string,
    transaction_fpds_sql_string,
    transaction_normalized_sql_string,
    transaction_search_create_sql_string,
)

from usaspending_api.recipient.models import DUNS, RecipientLookup, RecipientProfile
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    TransactionFABS,
    TransactionFPDS,
    TransactionNormalized,
)


TABLE_SPEC = {
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
        "custom_schema": "award_id LONG",
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
    # Additional definitions for use in testing
    "transaction_search_testing": {
        "model": TransactionSearch,
        "source_table": "transaction_search",
        "destination_database": "raw",
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": transaction_search_create_sql_string,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING",
    },
}


JDBC_URL_KEY = "DATABASE_URL"
SPARK_PARTITION_ROWS = CONFIG.SPARK_PARTITION_ROWS


class Command(BaseCommand):

    help = """
    This command reads data from a Postgres database table and inserts it into a corresponding Delta
    Table. As of now, it only supports a full reload of a table. All existing data will be deleted
    before new data is written.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--destination-table",
            type=str,
            required=True,
            help="The destination Delta Table to write the data",
            choices=list(TABLE_SPEC),
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

        table_spec = TABLE_SPEC[destination_table]
        destination_database = options["alt_db"] or table_spec["destination_database"]
        destination_table_name = options["alt_name"] or destination_table
        source_table = table_spec["source_table"]
        partition_column = table_spec["partition_column"]
        partition_column_type = table_spec["partition_column_type"]
        custom_schema = table_spec["custom_schema"]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"use {destination_database};")

        # Resolve JDBC URL for Source Database
        jdbc_url = get_jdbc_url()
        if not jdbc_url:
            raise RuntimeError(f"Couldn't find JDBC url, please properly configure your CONFIG.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

        # If a partition_column is present, read from jdbc using partitioning
        if partition_column:
            if partition_column_type == "numeric":
                is_numeric_partitioning_col = True
                is_date_partitioning_col = False
            elif partition_column_type == "date":
                is_numeric_partitioning_col = False
                is_date_partitioning_col = True
            else:
                raise ValueError("partition_column_type should be either 'numeric' or 'date'")

            # Read from table or view
            df = extract_db_data_frame(
                spark,
                get_jdbc_connection_properties(),
                jdbc_url,
                SPARK_PARTITION_ROWS,
                get_partition_bounds_sql(
                    source_table,
                    partition_column,
                    partition_column,
                    is_partitioning_col_unique=False,
                ),
                source_table,
                partition_column,
                is_numeric_partitioning_col=is_numeric_partitioning_col,
                is_date_partitioning_col=is_date_partitioning_col,
                custom_schema=custom_schema,
            )
        else:
            df = spark.read.options(customSchema=custom_schema).jdbc(
                url=jdbc_url,
                table=source_table,
                properties=get_jdbc_connection_properties(),
            )

        # Write to S3
        load_delta_table(spark, df, destination_table_name, True)
        if spark_created_by_command:
            spark.stop()
