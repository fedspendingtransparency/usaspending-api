from django.core.management.base import BaseCommand
from django.db import transaction
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jvm_logger,
)


class Command(BaseCommand):

    help = (
        "",
        "",
    )

    spark: SparkSession

    ETL_SQL_FILE_PATH = "usaspending_api/etl/management/sql/"

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
        logger = get_jvm_logger(self.spark, __name__)

        # Shallow Clone
        # TODO - Delete existing shallow clone (int.financial_accounts_by_awards)
        # TODO - Create new shallow clone (int.financial_accounts_by_awards)

        # Populate Temporary Tables
        # TODO - Rewrite existing c_to_d_linkage sql files as Spark SQL Temp Views
        # TODO - Add additional Temp View that contains FABA IDs and Null awards for deleted awards

        # Merge Temporary Tables into Shallow Clone (int.FABA)

        if spark_created_by_command:
            self.spark.stop()
