import logging

from django.core.management.base import BaseCommand
from pyspark.sql.types import StructType

from usaspending_api.awards.delta_models.award_id_lookup import AWARD_ID_LOOKUP_SCHEMA
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.config import CONFIG
from usaspending_api.etl.management.commands.archive_table_in_delta import TABLE_SPEC as ARCHIVE_TABLE_SPEC
from usaspending_api.etl.management.commands.load_query_to_delta import TABLE_SPEC as LOAD_QUERY_TABLE_SPEC
from usaspending_api.etl.management.commands.load_table_to_delta import TABLE_SPEC as LOAD_TABLE_TABLE_SPEC
from usaspending_api.transactions.delta_models.transaction_id_lookup import TRANSACTION_ID_LOOKUP_SCHEMA

TABLE_SPEC = {
    **ARCHIVE_TABLE_SPEC,
    **LOAD_TABLE_TABLE_SPEC,
    **LOAD_QUERY_TABLE_SPEC,
    "award_id_lookup": {
        "destination_database": "int",
        "delta_table_create_sql": AWARD_ID_LOOKUP_SCHEMA,
    },
    "transaction_id_lookup": {
        "destination_database": "int",
        "delta_table_create_sql": TRANSACTION_ID_LOOKUP_SCHEMA,
    },
}

logger = logging.getLogger(__name__)


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
            choices=list(TABLE_SPEC),
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
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)

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
        if isinstance(table_spec["delta_table_create_sql"], str):
            # Define Schema Using CREATE TABLE AS command
            spark.sql(
                TABLE_SPEC[destination_table]["delta_table_create_sql"].format(
                    DESTINATION_TABLE=destination_table_name,
                    DESTINATION_DATABASE=destination_database,
                    SPARK_S3_BUCKET=spark_s3_bucket,
                    DELTA_LAKE_S3_PATH=CONFIG.DELTA_LAKE_S3_PATH,
                )
            )
        elif isinstance(table_spec["delta_table_create_sql"], StructType):
            schema = table_spec["delta_table_create_sql"]
            partition_cols = table_spec.get("delta_table_create_partitions", [])
            df = spark.createDataFrame([], schema)
            df_writer = df.write.format("delta").mode("overwrite")

            if partition_cols:
                df_writer = df_writer.partitionBy(partition_cols)

            df_writer.option(
                "path",
                f"s3a://{spark_s3_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{destination_database}/{destination_table_name}",
            ).option("overwriteSchema", "true").saveAsTable(f"{destination_database}.{destination_table_name}")

        else:
            raise ValueError("Invalid Table Spec value for Delta Table creation.")

        if spark_created_by_command:
            spark.stop()
