from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

import logging

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG


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
            "--create-temp-tables",
            action="store_true",
            required=False,
            help="Whether to create proxy tables",
        )

    def log(self, label, content):
        self.logger_jvm.info(f"====== JVM LOGGER - {label} =======")
        self.logger_jvm.info(content)

        self.logger.info(f"====== Standard LOGGER - {label} =======")
        self.logger.info(content)


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

        # Setup Logger
        self.logger_jvm = get_jvm_logger(self.spark, __name__)
        self.logger = logging.getLogger("script")

        # Resolve Parameters
        if options['create_temp_tables']:
            create_ref_temp_views(self.spark, create_broker_views=True)
        
        # Run command and log
        df = self.spark.sql(
            rf"""
                SELECT
                    REGEXP_REPLACE(MD5(UPPER(
                        CASE
                            WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                                THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                            WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                                THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                            ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                        END
                    )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5') as joined_recipient_hash,
                    recipient_lookup.recipient_hash
                FROM
                    int.transaction_normalized
                LEFT OUTER JOIN
                    int.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
                LEFT OUTER JOIN
                    int.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
                LEFT OUTER JOIN
                    rpt.recipient_lookup ON (
                        recipient_lookup.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
                            CASE
                                WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                                    THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                                WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                                    THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                                ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                            END
                        )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
                    )
            """
        )
        self.log("Recipient Hash Comparison", df.show(500))



        if spark_created_by_command:
            self.spark.stop()
