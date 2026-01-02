import logging

from django.core.management.base import BaseCommand, CommandError
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.common.helpers.sql_helpers import split_sql_statements
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
    This command executes Spark SQL commands from either a file or a string.
    The resulting dataframe will be printed to standard out using the df.show() method
    """

    # Values defined in the handler
    spark: SparkSession

    def add_arguments(self, parser):

        parser.add_argument(
            "--sql",
            type=str,
            required=False,
            help="A string containing semicolon-separated Spark SQL statements.",
        )

        parser.add_argument(
            "--file",
            type=str,
            required=False,
            help="Path to file containing semicolon-separated SQL statements. Can be a local file path or S3/HTTP url",
        )

        parser.add_argument(
            "--create-temp-views",
            action="store_true",
            required=False,
            help="Controls whether all (USAs and Broker) temp views will be created before sql execution",
        )

        parser.add_argument(
            "--result-limit",
            type=int,
            required=False,
            default=20,
            help="Maximum number of result records to display from Pyspark dataframe.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            required=False,
            help="Print SQL statements without executing",
        )

    def handle(self, *args, **options):
        # Resolve Parameters
        sql_input = options.get("sql")
        file_path = options.get("file")
        create_temp_views = options.get("create_temp_views")
        result_limit = options.get("result_limit")
        dry_run = options.get("dry_run")

        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        # Prepare SQL Statements from either provided string or file
        if file_path and sql_input:
            raise CommandError("Cannot use both --sql and --file. Choose one.")
        elif file_path:
            with RetrieveFileFromUri(file_path).get_file_object(text=True) as f:
                sql_statement_string = f.read()
        elif sql_input:
            sql_statement_string = sql_input
        else:
            raise CommandError("Either --sql or --file must be provided")

        sql_statements = [query.strip() for query in split_sql_statements(sql_statement_string) if query.strip()]

        logger.info(f"Found {len(sql_statements)} SQL statement(s)")

        # Prepare Spark Session after variables parameters have been resolved and
        # SQL statements have been identified
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        if create_temp_views:
            create_ref_temp_views(self.spark, create_broker_views=True)

        # Execute SQL Statements
        for idx, statement in enumerate(sql_statements, 1):
            logger.info(f"--- Statement {idx} ---")
            logger.info(statement)

            if dry_run:
                logger.info("[DRY RUN - Not executed]")
            else:
                try:
                    df = self.spark.sql(statement)
                    df.show(result_limit)
                    logger.info("Executed successfully")
                except Exception as e:
                    logger.info(f"Error: {e}")

        if spark_created_by_command:
            self.spark.stop()
