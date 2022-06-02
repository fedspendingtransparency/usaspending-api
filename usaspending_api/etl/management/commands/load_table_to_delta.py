import os

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.config import CONFIG
from usaspending_api.common.etl.spark import (
    extract_db_data_frame,
    get_partition_bounds_sql,
    load_delta_table,
)
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jdbc_url_from_pg_uri,
    get_jvm_logger,
)
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC

JDBC_URL_KEY = "DATABASE_URL"
SPARK_PARTITION_ROWS = CONFIG.SPARK_PARTITION_ROWS


class Command(BaseCommand):

    help = """
    This command reads data from a Postgres database table and inserts it into a corresponding Delta
    Table. As of now, it only supports a full reload of a table. All existing data will be delted
    before new data is written.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--destination-table",
            type=str,
            required=True,
            help="The destination Delta Table to write the data",
            choices=list(TABLE_SPEC.keys()),
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
        destination_database = table_spec["destination_database"]
        source_table = table_spec["source_table"]
        partition_column = table_spec["partition_column"]
        partition_column_type = table_spec["partition_column_type"]
        custom_schema = table_spec["custom_schema"]

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"use {destination_database};")

        # Resolve JDBC URL for Source Database
        jdbc_url = os.environ.get(JDBC_URL_KEY)
        jdbc_url = get_jdbc_url_from_pg_uri(jdbc_url)
        if not jdbc_url:
            raise RuntimeError(f"Looking for JDBC URL passed to env var '{JDBC_URL_KEY}', but not set.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

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

        # Write to S3
        load_delta_table(spark, df, destination_table, True)
        if spark_created_by_command:
            spark.stop()
