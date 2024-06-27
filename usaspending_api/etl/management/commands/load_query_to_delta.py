from argparse import ArgumentTypeError
from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_broker_jdbc_url,
    get_jdbc_connection_properties,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.etl.management.helpers.table_specifications import DATABRICKS_GENERATED_TABLE_SPEC as TABLE_SPEC


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
            help="Whether or not the table will be updated incrementally. Requires `source_query_incremental` in TABLE_SPEC",
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

        # Resolve Parameters
        destination_table = options["destination_table"]
        table_spec = TABLE_SPEC[destination_table]
        self.destination_database = options["alt_db"] or table_spec["destination_database"]
        self.destination_table_name = options["alt_name"] or destination_table.split(".")[-1]
        load_query = table_spec["source_query"]

        if options["incremental"]:
            if table_spec.get("source_query_incremental") is None:
                raise ArgumentTypeError(
                    "When performing incremental loads, `source_query_incremental` must be present in TABLE_SPEC"
                )
            load_query = table_spec["source_query_incremental"]

        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        # Setup Logger
        logger = get_jvm_logger(self.spark, __name__)

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
            )
        )
