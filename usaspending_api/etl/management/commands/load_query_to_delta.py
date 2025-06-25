import logging
from argparse import ArgumentTypeError

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_broker_jdbc_url,
    get_jdbc_connection_properties,
)
from usaspending_api.config import CONFIG
from usaspending_api.disaster.delta_models import (
    COVID_FABA_SPENDING_DELTA_COLUMNS,
    COVID_FABA_SPENDING_POSTGRES_COLUMNS,
    covid_faba_spending_create_sql_string,
    covid_faba_spending_load_sql_strings,
)
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.download.delta_models.account_download import (
    ACCOUNT_DOWNLOAD_POSTGRES_COLUMNS,
    account_download_create_sql_string,
    account_download_load_sql_string,
)
from usaspending_api.recipient.delta_models import (
    RECIPIENT_LOOKUP_POSTGRES_COLUMNS,
    RECIPIENT_PROFILE_POSTGRES_COLUMNS,
    RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS,
    RPT_RECIPIENT_PROFILE_DELTA_COLUMNS,
    SAM_RECIPIENT_COLUMNS,
    SAM_RECIPIENT_POSTGRES_COLUMNS,
    recipient_lookup_load_sql_string_list,
    recipient_profile_create_sql_string,
    recipient_profile_load_sql_strings,
    rpt_recipient_lookup_create_sql_string,
    sam_recipient_create_sql_string,
    sam_recipient_load_sql_string,
)
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.search.delta_models.award_search import (
    AWARD_SEARCH_COLUMNS,
    AWARD_SEARCH_POSTGRES_COLUMNS,
    AWARD_SEARCH_POSTGRES_GOLD_COLUMNS,
    award_search_create_sql_string,
    award_search_incremental_load_sql_string,
    award_search_overwrite_load_sql_string,
)
from usaspending_api.search.delta_models.subaward_search import (
    SUBAWARD_SEARCH_COLUMNS,
    SUBAWARD_SEARCH_POSTGRES_COLUMNS,
    SUBAWARD_SEARCH_POSTGRES_VECTORS,
    subaward_search_create_sql_string,
    subaward_search_load_sql_string,
)
from usaspending_api.search.models import AwardSearch, SubawardSearch, SummaryStateView, TransactionSearch
from usaspending_api.settings import HOST
from usaspending_api.transactions.delta_models import (
    SUMMARY_STATE_VIEW_COLUMNS,
    SUMMARY_STATE_VIEW_POSTGRES_COLUMNS,
    TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS,
    TRANSACTION_SEARCH_POSTGRES_COLUMNS,
    TRANSACTION_SEARCH_POSTGRES_GOLD_COLUMNS,
    summary_state_view_create_sql_string,
    summary_state_view_load_sql_string,
    transaction_current_cd_lookup_create_sql_string,
    transaction_current_cd_lookup_load_sql_string,
    transaction_search_create_sql_string,
    transaction_search_incremental_load_sql_string,
    transaction_search_overwrite_load_sql_string,
)

AWARD_URL = f"{HOST}/award/" if "localhost" in HOST else f"https://{HOST}/award/"

logger = logging.getLogger(__name__)

TABLE_SPEC = {
    "award_search": {
        "model": AwardSearch,
        "is_from_broker": False,
        "source_query": award_search_overwrite_load_sql_string,
        "source_query_incremental": award_search_incremental_load_sql_string,
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
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "award_search_gold": {
        "model": AwardSearch,
        "is_from_broker": False,
        "source_query": award_search_overwrite_load_sql_string,
        "source_query_incremental": award_search_incremental_load_sql_string,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "award_search",
        "swap_schema": "rpt",
        "partition_column": "award_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": award_search_create_sql_string,
        "source_schema": AWARD_SEARCH_POSTGRES_GOLD_COLUMNS,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING, cfdas ARRAY<STRING>,"
        " tas_components ARRAY<STRING>",
        "column_names": list(AWARD_SEARCH_POSTGRES_GOLD_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "recipient_lookup": {
        "model": RecipientLookup,
        "is_from_broker": False,
        "source_query": recipient_lookup_load_sql_string_list,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "recipient_lookup",
        "swap_schema": "rpt",
        "partition_column": "recipient_hash",
        "partition_column_type": "string",
        "is_partition_column_unique": True,
        "delta_table_create_sql": rpt_recipient_lookup_create_sql_string,
        "source_schema": RECIPIENT_LOOKUP_POSTGRES_COLUMNS,
        "custom_schema": "recipient_hash STRING",
        "column_names": list(RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS),
        "postgres_seq_name": "recipient_lookup_id_seq",
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "recipient_profile": {
        "model": RecipientProfile,
        "is_from_broker": False,
        "source_query": recipient_profile_load_sql_strings,
        "source_query_incremental": None,
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
        "column_names": list(RPT_RECIPIENT_PROFILE_DELTA_COLUMNS),
        "postgres_seq_name": "recipient_profile_id_seq",
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "summary_state_view": {
        "model": SummaryStateView,
        "is_from_broker": False,
        "source_query": summary_state_view_load_sql_string,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "summary_state_view",
        "swap_schema": "rpt",
        "partition_column": "duh",
        "partition_column_type": "string",
        "is_partition_column_unique": True,
        "delta_table_create_sql": summary_state_view_create_sql_string,
        "source_schema": SUMMARY_STATE_VIEW_POSTGRES_COLUMNS,
        "custom_schema": "duh STRING",
        "column_names": list(SUMMARY_STATE_VIEW_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "sam_recipient": {
        "model": None,
        "is_from_broker": True,
        "source_query": sam_recipient_load_sql_string,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "int",
        "swap_table": "duns",
        "swap_schema": "int",
        "partition_column": "broker_duns_id",
        "partition_column_type": "string",
        "is_partition_column_unique": True,
        "delta_table_create_sql": sam_recipient_create_sql_string,
        "source_schema": SAM_RECIPIENT_POSTGRES_COLUMNS,
        "custom_schema": None,
        "column_names": list(SAM_RECIPIENT_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "transaction_search": {
        "model": TransactionSearch,
        "is_from_broker": False,
        "source_query": transaction_search_overwrite_load_sql_string,
        "source_query_incremental": transaction_search_incremental_load_sql_string,
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
        "column_names": list(TRANSACTION_SEARCH_POSTGRES_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "transaction_search_gold": {
        "model": TransactionSearch,
        "is_from_broker": False,
        "source_query": transaction_search_overwrite_load_sql_string,
        "source_query_incremental": transaction_search_incremental_load_sql_string,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "transaction_search",
        "swap_schema": "rpt",
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_search_create_sql_string,
        "source_schema": TRANSACTION_SEARCH_POSTGRES_GOLD_COLUMNS,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING, parent_recipient_hash STRING",
        "column_names": list(TRANSACTION_SEARCH_POSTGRES_GOLD_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": {
            "partition_keys": ["is_fpds"],
            "partitioning_form": "LIST",
            "partitions": [
                {"table_suffix": "_fpds", "partitioning_clause": "FOR VALUES IN (TRUE)"},
                {"table_suffix": "_fabs", "partitioning_clause": "FOR VALUES IN (FALSE)"},
            ],
        },
    },
    "transaction_current_cd_lookup": {
        "model": None,
        "is_from_broker": False,
        "source_query": transaction_current_cd_lookup_load_sql_string,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "int",
        "swap_table": "transaction_current_cd_lookup",
        "swap_schema": "int",
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_current_cd_lookup_create_sql_string,
        "source_schema": TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS,
        "custom_schema": "",
        "column_names": list(TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "subaward_search": {
        "model": SubawardSearch,
        "is_from_broker": False,
        "source_query": subaward_search_load_sql_string,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "subaward_search",
        "swap_schema": "rpt",
        "partition_column": "broker_subaward_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": subaward_search_create_sql_string,
        "source_schema": SUBAWARD_SEARCH_POSTGRES_COLUMNS,
        "custom_schema": "treasury_account_identifiers ARRAY<INTEGER>",
        "column_names": list(SUBAWARD_SEARCH_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": SUBAWARD_SEARCH_POSTGRES_VECTORS,
        "postgres_partition_spec": None,
    },
    "covid_faba_spending": {
        "model": CovidFABASpending,
        "is_from_broker": False,
        "source_query": covid_faba_spending_load_sql_strings,
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": "covid_faba_spending",
        "swap_schema": "rpt",
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": False,
        "delta_table_create_sql": covid_faba_spending_create_sql_string,
        "source_schema": COVID_FABA_SPENDING_POSTGRES_COLUMNS,
        "custom_schema": None,
        "column_names": list(COVID_FABA_SPENDING_DELTA_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
    },
    "account_download": {
        "model": None,
        "is_from_broker": False,
        "source_query": [account_download_load_sql_string],
        "source_query_incremental": None,
        "source_database": None,
        "source_table": None,
        "destination_database": "rpt",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "financial_accounts_by_awards_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": False,
        "delta_table_create_sql": account_download_create_sql_string,
        "source_schema": ACCOUNT_DOWNLOAD_POSTGRES_COLUMNS,
        "custom_schema": None,
        "column_names": list(ACCOUNT_DOWNLOAD_POSTGRES_COLUMNS),
        "postgres_seq_name": None,
        "tsvectors": None,
        "postgres_partition_spec": None,
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
        parser.add_argument(
            "--incremental",
            action="store_true",
            required=False,
            help="Whether or note the table will be updated incrementally",
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

        # Resolve Parameters
        destination_table = options["destination_table"]
        table_spec = TABLE_SPEC[destination_table]
        self.destination_database = options["alt_db"] or table_spec["destination_database"]
        self.destination_table_name = options["alt_name"] or destination_table.split(".")[-1]
        source_query_key = "source_query_incremental" if options["incremental"] else "source_query"
        load_query = table_spec.get(source_query_key)
        if load_query is None:
            raise ArgumentTypeError(f"Invalid source query. `{source_query_key}` must be specified in the TABLE_SPEC.")

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {self.destination_database}")
        self.spark.sql(f"use {self.destination_database};")

        # Create User Defined Functions if needed
        if table_spec.get("user_defined_functions"):
            for udf_args in table_spec["user_defined_functions"]:
                self.spark.udf.register(**udf_args)

        create_ref_temp_views(self.spark, create_broker_views=True)

        if isinstance(load_query, list):
            for index, query in enumerate(load_query):
                logger.info(f"Running query number: {index + 1}\nPreview of query: {query[:100]}")
                self.run_spark_sql(query)
        else:
            self.run_spark_sql(load_query)

        if spark_created_by_command:
            self.spark.stop()

    def run_spark_sql(self, query):
        jdbc_conn_props = get_jdbc_connection_properties()
        self.spark.sql(
            query.format(
                DESTINATION_DATABASE=self.destination_database,
                DESTINATION_TABLE=self.destination_table_name,
                DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
                JDBC_DRIVER=jdbc_conn_props["driver"],
                JDBC_FETCHSIZE=jdbc_conn_props["fetchsize"],
                JDBC_URL=get_broker_jdbc_url(),
                AWARD_URL=AWARD_URL,
            )
        )
