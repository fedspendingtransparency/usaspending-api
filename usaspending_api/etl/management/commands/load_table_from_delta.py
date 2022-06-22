from django import db
from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jdbc_url,
    get_jvm_logger,
)
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC


class Command(BaseCommand):

    help = """
    This command reads data from a Delta table and copies it into a corresponding Postgres database (under a temp name).
    As of now, it only supports a full reload of a table. All existing data will be deleted before new data is written.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--delta-table",
            type=str,
            required=True,
            help="The source Delta Table to read the data",
            choices=list(TABLE_SPEC.keys()),
        )
        parser.add_argument(
            "--drop-recreate",
            action="store_true",
            help="If provided, drops the temp table if exists and makes it from scratch. By default, the script"
            " just truncates and repopulates if it exists, which is more efficient. However, the schema may have"
            "updated since or you may just want to recreate it.",
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
        delta_table = options["delta_table"]
        drop_recreate = options["drop_recreate"]

        table_spec = TABLE_SPEC[delta_table]
        destination_database = table_spec["destination_database"]
        source_table = table_spec["source_table"]
        custom_schema = table_spec["custom_schema"]

        temp_schema = "temp"
        temp_destination_table = f"{temp_schema}.{source_table}_temp"

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"use {destination_database};")

        # Resolve JDBC URL for Source Database
        jdbc_url = get_jdbc_url()
        if not jdbc_url:
            raise RuntimeError(f"Couldn't find JDBC url, please properly configure your CONFIG.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

        if drop_recreate:
            logger.info(f"Drop-recreate argument provided. Dropping {delta_table} first if exists.")
            # If the schema has changed and we need to do a complete reload, just drop the table and rebuild it
            clear_table_sql = f"DROP {temp_destination_table} IF EXISTS"
            with db.connection.cursor() as cursor:
                cursor.execute(clear_table_sql)
            logger.info(f"{delta_table} dropped. Now creating.")

        # Read from Delta
        df = spark.sql(f"SELECT * FROM {delta_table}")

        # Write to Postgres
        logger.info(f"LOAD (START): Loading data from Delta table {delta_table} to {temp_destination_table}")
        df.write.options(customSchema=custom_schema, truncate=True).jdbc(
            url=jdbc_url, table=temp_destination_table, mode="overwrite", properties=get_jdbc_connection_properties()
        )
        logger.info(f"LOAD (FINISH): Loaded data from Delta table {delta_table} to {temp_destination_table}")

        if spark_created_by_command:
            spark.stop()
