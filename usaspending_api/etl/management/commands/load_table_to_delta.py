import os
import sys

from django.core.management.base import BaseCommand

from usaspending_api.broker.helpers.last_load_date import get_last_load_date
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple
from usaspending_api.common.etl.spark import (
    extract_db_data_frame,
    get_partition_bounds_sql,
    load_delta_table,
    merge_delta_table,
)
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_jdbc_url, get_jvm_logger
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC

from pyspark.sql import SparkSession

JDBC_URL_KEY = "JDBC_URL"

PARTITION_ROWS = 10000 * 16
# Abort processing the data if it would yield more than this many partitions to process as individual tasks
MAX_PARTITIONS = 100000
JDBC_CONN_PROPS = {"driver": "org.postgresql.Driver", "fetchsize": str(PARTITION_ROWS)}


class Command(BaseCommand):

    help = """

    """

    def add_arguments(self, parser):
        parser.add_argument("--destination-table", type=str, required=True, help="", choices=list(TABLE_SPEC.keys()))

        parser.add_argument(
            "--full-reload",
            action="store_true",
            default=False,
            help="Empties the USAspending subaward and broker_subaward tables before loading.",
        )

    def handle(self, *args, **options):
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.warehouse.dir": "/project/warehouse_dir",
        }
        spark = configure_spark_session(**extra_conf)  # type: SparkSession

        # Setup Logger
        logger = get_jvm_logger(spark)

        # Resolve Parameters
        destination_table = options["destination_table"]
        full_reload = options["full_reload"]

        table_spec = TABLE_SPEC[destination_table]
        destination_database = table_spec["destination_database"]
        source_table = table_spec["source_table"]
        external_load_date_key = table_spec["external_load_date_key"]
        partition_column = table_spec["partition_column"]
        merge_column = table_spec["merge_column"]
        last_update_column = table_spec["last_update_column"]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"use {destination_database};")

        jdbc_url = os.environ.get(JDBC_URL_KEY)

        if not jdbc_url:
            raise RuntimeError(f"Looking for JDBC URL passed to env var '{JDBC_URL_KEY}', but not set.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

        # Create temporary view on top of table that includes date predicate
        source_entity = source_table
        if not full_reload:
            last_load_date = get_last_load_date(external_load_date_key)
            source_entity = f"{source_table}_LATEST"
            execute_sql_simple(
                f"CREATE OR REPLACE VIEW {source_entity} AS SELECT * FROM {source_table} WHERE {last_update_column} > {last_load_date}"
            )

        # Read from table or view
        source_assistance_transaction_df = extract_db_data_frame(
            spark,
            JDBC_CONN_PROPS,
            get_jdbc_url(),
            PARTITION_ROWS,
            get_partition_bounds_sql(
                # TODO Correct this to point to source table info
                source_table,
                partition_column,
                partition_column,
                is_partitioning_col_unique=False,
            ),
            source_entity,
            partition_column,
        )

        # Write to S3
        if not full_reload:
            load_delta_table(spark, source_assistance_transaction_df, f"{destination_table}", True)
            execute_sql_simple(f"DROP VIEW {source_entity}")
            # TODO - update external load date
        else:
            merge_delta_table(
                spark,
                source_assistance_transaction_df,
                f"{destination_table}",
                merge_column,
            )

        spark.stop()
