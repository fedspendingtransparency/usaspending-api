import logging

from django.core.management import BaseCommand

from usaspending_api.awards.delta_models import (
    AWARDS_COLUMNS,
    awards_sql_string,
    FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS,
    financial_accounts_by_awards_sql_string,
    BROKER_SUBAWARDS_COLUMNS,
    broker_subawards_sql_string,
)
from usaspending_api.broker.delta_models.broker_zips import ZIPS_COLUMNS, zips_sql_string
from usaspending_api.common.etl.spark import extract_db_data_frame, get_partition_bounds_sql, load_delta_table
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
    get_broker_jdbc_url,
)
from usaspending_api.config import CONFIG
from usaspending_api.recipient.delta_models import (
    RECIPIENT_LOOKUP_COLUMNS,
    recipient_lookup_create_sql_string,
    recipient_profile_create_sql_string,
    RECIPIENT_PROFILE_DELTA_COLUMNS,
    SAM_RECIPIENT_COLUMNS,
    sam_recipient_create_sql_string,
)
from usaspending_api.search.models import TransactionSearch, AwardSearch
from usaspending_api.transactions.delta_models import (
    DETACHED_AWARD_PROCUREMENT_DELTA_COLUMNS,
    detached_award_procurement_create_sql_string,
    TRANSACTION_FABS_VIEW_COLUMNS,
    transaction_fabs_sql_string,
    TRANSACTION_FPDS_VIEW_COLUMNS,
    transaction_fpds_sql_string,
    TRANSACTION_NORMALIZED_COLUMNS,
    transaction_normalized_sql_string,
    TRANSACTION_SEARCH_POSTGRES_COLUMNS,
    transaction_search_create_sql_string,
    PUBLISHED_FABS_COLUMNS,
    published_fabs_create_sql_string,
)
from usaspending_api.transactions.models import SourceAssistanceTransaction
from usaspending_api.transactions.models import SourceProcurementTransaction
from usaspending_api.search.delta_models.award_search import award_search_create_sql_string, AWARD_SEARCH_COLUMNS

from usaspending_api.recipient.models import DUNS, RecipientLookup, RecipientProfile
from usaspending_api.awards.models import (
    Award,
    FinancialAccountsByAwards,
    TransactionFABS,
    TransactionFPDS,
    TransactionNormalized,
)

logger = logging.getLogger(__name__)

TABLE_SPEC = {
    "awards": {
        "model": Award,
        "is_from_broker": False,
        "source_table": "vw_awards",
        "source_database": "rpt",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": awards_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(AWARDS_COLUMNS),
        "tsvectors": None,
    },
    "detached_award_procurement": {
        "model": SourceProcurementTransaction,
        "is_from_broker": False,
        "source_table": "source_procurement_transaction",
        "source_database": "raw",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "detached_award_procurement_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": detached_award_procurement_create_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(DETACHED_AWARD_PROCUREMENT_DELTA_COLUMNS),
        "tsvectors": None,
    },
    "financial_accounts_by_awards": {
        "model": FinancialAccountsByAwards,
        "is_from_broker": False,
        "source_table": "financial_accounts_by_awards",
        "source_database": "public",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "financial_accounts_by_awards_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": financial_accounts_by_awards_sql_string,
        "source_schema": None,
        "custom_schema": "award_id LONG",
        "column_names": list(FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS),
        "tsvectors": None,
    },
    "transaction_fabs": {
        "model": TransactionFABS,
        "is_from_broker": False,
        "source_table": "vw_transaction_fabs",
        "source_database": "int",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_fabs_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": TRANSACTION_FABS_VIEW_COLUMNS,
        "tsvectors": None,
    },
    "published_fabs": {
        "model": SourceAssistanceTransaction,
        "is_from_broker": False,
        "source_table": "source_assistance_transaction",
        "source_database": "raw",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "published_fabs_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": published_fabs_create_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(PUBLISHED_FABS_COLUMNS),
        "tsvectors": None,
    },
    "transaction_fpds": {
        "model": TransactionFPDS,
        "is_from_broker": False,
        "source_table": "vw_transaction_fpds",
        "source_database": "int",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_fpds_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": TRANSACTION_FPDS_VIEW_COLUMNS,
        "tsvectors": None,
    },
    "transaction_normalized": {
        "model": TransactionNormalized,
        "is_from_broker": False,
        "source_table": "vw_transaction_normalized",
        "source_database": "int",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_normalized_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(TRANSACTION_NORMALIZED_COLUMNS),
        "tsvectors": None,
    },
    # Tables loaded in from the Broker
    "subaward": {
        "model": None,
        "is_from_broker": True,
        "source_table": "subaward",
        "source_database": None,
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": broker_subawards_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(BROKER_SUBAWARDS_COLUMNS),
        "tsvectors": None,
    },
    "zips": {
        "model": None,
        "is_from_broker": True,
        "source_table": "zips",
        "source_database": None,
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "zips_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": zips_sql_string,
        "source_schema": None,
        "custom_schema": "",
        "column_names": list(ZIPS_COLUMNS),
        "tsvectors": None,
    },
    # Additional definitions for use in testing;
    # These are copies of Views / Materialized Views / Tables from Postgres to Spark to aid in
    # data comparison between current Postgres data and the data transformed via Spark.
    "award_search_testing": {
        "model": AwardSearch,
        "is_from_broker": False,
        "source_table": "award_search",
        "source_database": None,
        "destination_database": "rpt",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "award_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": award_search_create_sql_string,
        "source_schema": None,
        "custom_schema": "total_covid_outlay NUMERIC(23,2), total_covid_obligation NUMERIC(23,2), recipient_hash "
        "STRING, federal_accounts STRING, cfdas ARRAY<STRING>, tas_components ARRAY<STRING>",
        "column_names": list(AWARD_SEARCH_COLUMNS),
        "tsvectors": None,
    },
    "recipient_lookup_testing": {
        "model": RecipientLookup,
        "is_from_broker": False,
        "source_table": "recipient_lookup",
        "source_database": "rpt",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": recipient_lookup_create_sql_string,
        "source_schema": None,
        "custom_schema": "recipient_hash STRING",
        "column_names": list(RECIPIENT_LOOKUP_COLUMNS),
        "tsvectors": None,
    },
    "recipient_profile_testing": {
        "model": RecipientProfile,
        "is_from_broker": False,
        "source_table": "recipient_profile",
        "source_database": "rpt",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "id",
        "partition_column_type": "numeric",
        "delta_table_create_sql": recipient_profile_create_sql_string,
        "is_partition_column_unique": True,
        "source_schema": None,
        "custom_schema": "recipient_hash STRING",
        "column_names": list(RECIPIENT_PROFILE_DELTA_COLUMNS),
        "tsvectors": None,
    },
    "sam_recipient_testing": {
        "model": DUNS,
        "is_from_broker": False,
        "source_table": "duns",
        "source_database": "int",
        "destination_database": "raw",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": None,
        "partition_column_type": None,
        "is_partition_column_unique": False,
        "delta_table_create_sql": sam_recipient_create_sql_string,
        "source_schema": None,
        "custom_schema": "broker_duns_id STRING, business_types_codes ARRAY<STRING>",
        "column_names": list(SAM_RECIPIENT_COLUMNS),
        "tsvectors": None,
    },
    "transaction_search_testing": {
        "model": TransactionSearch,
        "is_from_broker": False,
        "source_table": "transaction_search",
        "source_database": None,
        "destination_database": "test",
        "swap_table": None,
        "swap_schema": None,
        "partition_column": "transaction_id",
        "partition_column_type": "numeric",
        "is_partition_column_unique": True,
        "delta_table_create_sql": transaction_search_create_sql_string,
        "source_schema": None,
        "custom_schema": "recipient_hash STRING, federal_accounts STRING, parent_recipient_hash STRING",
        "column_names": list(TRANSACTION_SEARCH_POSTGRES_COLUMNS),
        "tsvectors": None,
    },
}

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
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)

        # Resolve Parameters
        destination_table = options["destination_table"]

        table_spec = TABLE_SPEC[destination_table]
        is_from_broker = table_spec["is_from_broker"]
        destination_database = options["alt_db"] or table_spec["destination_database"]
        destination_table_name = options["alt_name"] or destination_table
        source_table = table_spec["source_table"]
        partition_column = table_spec["partition_column"]
        partition_column_type = table_spec["partition_column_type"]
        is_partition_column_unique = table_spec["is_partition_column_unique"]
        custom_schema = table_spec["custom_schema"]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"use {destination_database};")

        # Resolve JDBC URL for Source Database
        jdbc_url = get_usas_jdbc_url() if not is_from_broker else get_broker_jdbc_url()
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
                spark=spark,
                conn_props=get_jdbc_connection_properties(),
                jdbc_url=jdbc_url,
                partition_rows=SPARK_PARTITION_ROWS,
                min_max_sql=get_partition_bounds_sql(
                    table_name=source_table,
                    partitioning_col_name=partition_column,
                    partitioning_col_alias=partition_column,
                    is_partitioning_col_unique=is_partition_column_unique,
                ),
                table=source_table,
                partitioning_col=partition_column,
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

        # Make sure that the column order defined in the Delta table schema matches
        # that of the Spark dataframe used to pull from the Postgres table. While not
        # always needed, this should help to prevent any future mismatch between the two.
        if table_spec.get("column_names"):
            df = df.select(table_spec.get("column_names"))

        # Write to S3
        load_delta_table(spark, df, destination_table_name, True)
        if spark_created_by_command:
            spark.stop()
