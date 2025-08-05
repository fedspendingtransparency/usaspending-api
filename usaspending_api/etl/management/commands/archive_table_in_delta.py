import logging
import psycopg2

from datetime import datetime, timedelta
from django.core.management.base import BaseCommand

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.common.etl.spark import load_delta_table
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
)
from usaspending_api.download.delta_models.download_job import download_job_create_sql_string

logger = logging.getLogger(__name__)

TABLE_SPEC = {
    "download_job": {
        "destination_database": "arc",
        "destination_table": "download_job",
        "archive_date_field": "update_date",
        "source_table": "download_job",
        "source_database": "public",
        "delta_table_create_sql": download_job_create_sql_string,
    }
}


class Command(BaseCommand):

    help = """
    Copies records older than "--archive-period" days ago from Postgres to Delta Lake then deletes
    those records from Postgres.
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
            "--archive-period",
            type=int,
            required=False,
            default=30,
            help="The number of days to keep data in base table before archiving",
        )
        parser.add_argument(
            "--alt-db",
            type=str,
            required=False,
            help="An alternate Delta Database (aka schema) in which to archive this table, overriding the TABLE_SPEC's destination_database",
        )
        parser.add_argument(
            "--alt-name",
            type=str,
            required=False,
            help="An alternate Delta Table name which to archive this table, overriding the destination_table",
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

        # Setup Spark Connection
        spark = get_active_spark_session()
        spark_created_by_command = False
        if spark is None:
            spark_created_by_command = True
            spark = configure_spark_session(**extra_conf, spark_context=spark)

        # Resolve Parameters
        destination_table = options["destination_table"]
        archive_period = options["archive_period"]

        table_spec = TABLE_SPEC[destination_table]
        destination_database = options["alt_db"] or table_spec["destination_database"]
        destination_table_name = options["alt_name"] or destination_table
        source_table = table_spec["source_table"]
        source_database = table_spec["source_database"]
        qualified_source_table = f"{source_database}.{source_table}"
        archive_date_field = table_spec["archive_date_field"]

        archive_date = datetime.now() - timedelta(days=archive_period)
        archive_date_string = archive_date.strftime("%Y-%m-%d")

        # Set the database that will be interacted with for all Delta Lake table Spark-based activity
        logger.info(f"Using Spark Database: {destination_database}")
        spark.sql(f"create database if not exists {destination_database};")
        spark.sql(f"use {destination_database};")

        # Resolve JDBC URL for Source Database
        jdbc_url = get_usas_jdbc_url()
        if not jdbc_url:
            raise RuntimeError(f"Couldn't find JDBC url, please properly configure your CONFIG.")
        if not jdbc_url.startswith("jdbc:postgresql://"):
            raise ValueError("JDBC URL given is not in postgres JDBC URL format (e.g. jdbc:postgresql://...")

        # Retrieve data from Postgres
        query_with_predicate = (
            f"(SELECT * FROM {qualified_source_table} WHERE {archive_date_field} < '{archive_date_string}') AS tmp"
        )

        df = spark.read.jdbc(
            url=jdbc_url,
            table=query_with_predicate,
            properties=get_jdbc_connection_properties(),
        )

        # Write data to Delta Lake in Append Mode
        load_delta_table(spark, df, destination_table_name, overwrite=False)
        archived_count = df.count()
        logger.info(f"Archived {archived_count} records from the {qualified_source_table}")

        # Delete data from
        with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {qualified_source_table} WHERE {archive_date_field} < '{archive_date_string}'"
                )
                deleted_count = cursor.rowcount

        logger.info(f"Deleted {deleted_count} records from the {qualified_source_table} table")

        # Shut down spark
        if spark_created_by_command:
            spark.stop()
