from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.recipient.delta_models import (
    RECIPIENT_LOOKUP_DELTA_COLUMNS,
    recipient_lookup_load_sql_string_list,
    RECIPIENT_LOOKUP_POSTGRES_COLUMNS,
    recipient_profile_create_sql_string,
    recipient_profile_load_sql_strings,
    RECIPIENT_PROFILE_POSTGRES_COLUMNS,
    rpt_recipient_lookup_create_sql_string,
    RECIPIENT_PROFILE_DELTA_COLUMNS,
)
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.search.delta_models.award_search import (
    AWARD_SEARCH_COLUMNS,
    award_search_create_sql_string,
    award_search_load_sql_string,
    AWARD_SEARCH_POSTGRES_COLUMNS,
)
from usaspending_api.search.delta_models.subaward_search import (
    SUBAWARD_SEARCH_COLUMNS,
    subaward_search_create_sql_string,
    subaward_search_load_sql_string,
    SUBAWARD_SEARCH_POSTGRES_COLUMNS,
)
from usaspending_api.search.models import TransactionSearch, AwardSearch
from usaspending_api.transactions.delta_models import (
    TRANSACTION_SEARCH_COLUMNS,
    transaction_search_create_sql_string,
    transaction_search_load_sql_string,
    TRANSACTION_SEARCH_POSTGRES_COLUMNS,
)

TABLE_SPEC = {
    "award_search": {
        "model": AwardSearch,
        "is_from_broker": False,
        "source_query": award_search_load_sql_string,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "award_search",
        "swap_schema": "rpt",
        "partition_column": "award_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": award_search_create_sql_string,
        "source_schema": AWARD_SEARCH_POSTGRES_COLUMNS,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING, cfdas ARRAY<STRING>,"
        " tas_components ARRAY<STRING>",
        "column_names": list(AWARD_SEARCH_COLUMNS),
    },
    "recipient_lookup": {
        "model": RecipientLookup,
        "is_from_broker": False,
        "source_query": recipient_lookup_load_sql_string_list,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "recipient_hash",
        "partition_column_type": "string",
        "is_partition_column_unique": True,
        "delta_table_create_sql": rpt_recipient_lookup_create_sql_string,
        "source_schema": RECIPIENT_LOOKUP_POSTGRES_COLUMNS,
        "custom_schema": "recipient_hash STRING",
        "column_names": list(RECIPIENT_LOOKUP_DELTA_COLUMNS),
    },
    "recipient_profile": {
        "model": RecipientProfile,
        "source_query": recipient_profile_load_sql_strings,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "recipient_profile",
        "swap_schema": "rpt",
        "partition_column": "recipient_hash",  # This isn't used for anything
        "partition_column_type": "string",
        "is_partition_column_unique": False,
        "delta_table_create_sql": recipient_profile_create_sql_string,
        "source_schema": RECIPIENT_PROFILE_POSTGRES_COLUMNS,
        "custom_schema": "recipient_hash STRING",
        "column_names": [x for x in list(RECIPIENT_PROFILE_DELTA_COLUMNS) if x != "id"],
    },
    "transaction_search": {
        "model": TransactionSearch,
        "is_from_broker": False,
        "source_query": transaction_search_load_sql_string,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "transaction_search",
        "swap_schema": "rpt",
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_search_create_sql_string,
        "source_schema": TRANSACTION_SEARCH_POSTGRES_COLUMNS,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING, parent_recipient_hash STRING",
        "column_names": list(TRANSACTION_SEARCH_COLUMNS),
    },
    "subaward_search": {
        "model": None,
        "is_from_broker": False,
        "source_query": subaward_search_load_sql_string,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "broker_subaward_id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": subaward_search_create_sql_string,
        "source_schema": SUBAWARD_SEARCH_POSTGRES_COLUMNS,
        "custom_schema": "treasury_account_identifiers ARRAY<INTEGER>",
        "column_names": list(SUBAWARD_SEARCH_COLUMNS),
    },
}


class Command(BaseCommand):

    help = """
    This command reads data via a Spark SQL query that relies on delta tables that have already been loaded paired
    with temporary views of tables in a Postgres database. As of now, it only supports a full reload of a table.
    All existing data will be deleted before new data is written.
    """

    # Values defined in the handler
    destination_database: str
    destination_table_name: str
    spark: SparkSession

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
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        # Setup Logger
        logger = get_jvm_logger(self.spark, __name__)

        # Resolve Parameters
        destination_table = options["destination_table"]
        table_spec = TABLE_SPEC[destination_table]
        self.destination_database = options["alt_db"] or table_spec["destination_database"]
        self.destination_table_name = options["alt_name"] or destination_table.split(".")[-1]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {self.destination_database}")
        self.spark.sql(f"use {self.destination_database};")

        # Create User Defined Functions if needed
        if table_spec.get("user_defined_functions"):
            for udf_args in table_spec["user_defined_functions"]:
                self.spark.udf.register(**udf_args)

        create_ref_temp_views(self.spark)

        load_query = table_spec["source_query"]
        if isinstance(load_query, list):
            for index, query in enumerate(load_query):
                logger.info(f"Running query number: {index + 1}\nPreview of query: {query[:100]}")
                self.run_spark_sql(query)
        else:
            self.run_spark_sql(load_query)

        if spark_created_by_command:
            self.spark.stop()

    def run_spark_sql(self, query):
        self.spark.sql(
            query.format(
                DESTINATION_DATABASE=self.destination_database,
                DESTINATION_TABLE=self.destination_table_name,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
            )
        )
