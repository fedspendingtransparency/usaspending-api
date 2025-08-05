import logging

from datetime import datetime

from django.core.management import BaseCommand, call_command

from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = """
    This command checks if the zips table from Broker has been updated and, if it has, calls the
    load_table_to_delta command to replace the existing Delta table with the updated table from Broker
    """

    def add_arguments(self, parser):
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

        logger.info("Creating global_temp views")
        create_ref_temp_views(spark=spark, create_broker_views=True)

        usaspending_last_load_date = get_last_load_date("zips")
        options["destination_table"] = "zips"

        # Get the most recent updated_at datetime from zips_grouped since it's quicker to query than the zips table
        # Note: Will be datetime or None
        broker_last_load_date = spark.sql("SELECT MAX(updated_at) FROM global_temp.zips_grouped").first()[0]

        # If the zips table from Broker has a newer updated_at datetime than raw.zips does, then call load_table_to_delta
        if (
            usaspending_last_load_date is None
            or broker_last_load_date is None
            or broker_last_load_date > usaspending_last_load_date
        ):
            logger.info("Broker's zips table is more recent, running load_table_to_delta")
            call_command("load_table_to_delta", *args, **options)
            update_last_load_date("zips", datetime.now())

        if spark_created_by_command:
            spark.stop()
