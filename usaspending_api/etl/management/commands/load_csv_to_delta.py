import logging
from dataclasses import dataclass

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.config import CONFIG
from usaspending_api.references.delta_models.world_cities import schema as world_cities_schema


logger = logging.getLogger(__name__)


@dataclass
class CsvTableMetadata:
    db_name: str
    table_name: str
    schema: StructType
    source_path: str


destination_tables = {
    "world_cities": CsvTableMetadata(
        db_name="raw",
        table_name="world_cities",
        schema=world_cities_schema,
        source_path=f"s3a://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.SPARK_CSV_S3_PATH}/worldcities.csv",
    )
}


class Command(BaseCommand):
    help = """
    This command reads a csv file into a spark table. It only supports a full reload of a table.
    All existing data will be deleted before new data is written.
    """

    # Values defined in the handler
    csv_metadata: CsvTableMetadata
    spark_s3_bucket: str
    spark: SparkSession

    def add_arguments(self, parser):
        parser.add_argument(
            "--destination-table",
            type=str,
            required=True,
            help="The destination spark table to write the data",
            choices=list(destination_tables),
        )
        parser.add_argument(
            "--alt-db",
            type=str,
            required=False,
            help="An alternate database (aka schema) in which to create this table, overriding the destination-table db name",
        )
        parser.add_argument(
            "--alt-name",
            type=str,
            required=False,
            help="An alternate delta table name for the created table, overriding the destination-table table name",
        )
        parser.add_argument(
            "--alt-source-path",
            type=str,
            required=False,
            help="An alternate path for the source csv file, overriding the destination-table path.  Supports the s3a:// protocol",
        )
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3",
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
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)

        # Resolve Parameters
        self.csv_metadata = destination_tables[options["destination_table"]]
        if options["alt_db"]:
            self.csv_metadata.db_name = options["alt_db"]
        if options["alt_name"]:
            self.csv_metadata.db_name = options["alt_name"]
        if options["alt_source_path"]:
            self.csv_metadata.source_path = options["alt_source_path"]
        if options["spark_s3_bucket"]:
            self.spark_s3_bucket = options["spark_s3_bucket"]

        self.load_csv()

        if spark_created_by_command:
            self.spark.stop()

    def load_csv(self):
        delta_path = f"s3a://{self.spark_s3_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/{self.csv_metadata.db_name}/{self.csv_metadata.table_name}"
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.csv_metadata.db_name};")
        df = self.spark.read.csv(self.csv_metadata.source_path, header=True, schema=self.csv_metadata.schema)
        df.write.saveAsTable(
            f"{self.csv_metadata.db_name}.{self.csv_metadata.table_name}",
            mode="overwrite",
            format="delta",
            path=delta_path,
            options={"overwriteSchema": True},
        )
