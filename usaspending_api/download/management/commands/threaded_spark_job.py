import concurrent.futures
import itertools
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from django.conf import settings
from django.core.management import BaseCommand
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ArrayType, StringType, IntegerType
from usaspending_api.common.helpers.download_csv_strategies import CSVDownloadMetadata, SparkToCSVStrategy
from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3, delete_s3_objects
from usaspending_api.common.helpers.spark_helpers import get_active_spark_session, configure_spark_session
from usaspending_api.common.helpers.timing_helpers import Timer as BaseTimer
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)

"""
Docs:
- https://community.databricks.com/t5/data-engineering/enabling-fair-scheduler-from-databricks-notebook/td-p/31267
- Example: https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
- https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
- https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor
- https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future
- https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.result
- https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.as_completed

"""


class SchedulerType(str, Enum):
    FAIR = "FAIR"
    FIFO = "FIFO"


@dataclass
class DownloadCategory:
    download_unique_id: int
    table_name: str
    agency_abbreviation: str
    fiscal_year: int

    def __str__(self):
        return f"Agency: {self.agency_abbreviation} | Fiscal Year: {self.fiscal_year}"


class Timer(BaseTimer):
    def __init__(self, message=None):
        super().__init__(message=message, success_logger=logger.info, failure_logger=logger.error)


class Command(BaseCommand):
    help = "Proof of concept to test threads when performing multiple Spark jobs that involve I/O"

    spark: SparkSession

    def add_arguments(self, parser):
        parser.add_argument(
            "-n",
            "--num-threads",
            dest="num_threads",
            type=int,
            help="Number of threads allocated to the pool",
            required=True,
        )
        parser.add_argument(
            "-s",
            "--schedular",
            dest="schedular",
            type=str,
            help="Job scheduler used by Spark",
            choices=[val for val in SchedulerType],
            required=True,
        )
        parser.add_argument(
            "-a",
            "--agencies",
            dest="agencies",
            type=str,
            help="Comma separated list of agency abbreviations to pull data for",
            required=True,
        )
        parser.add_argument(
            "-fy",
            "--fiscal-years",
            dest="fiscal_years",
            type=str,
            help="Comma separated list of fiscal years to pull data for",
            required=True,
        )
        parser.add_argument(
            "-t",
            "--table-name",
            dest="table_name",
            type=str,
            help=(
                "Table name registered in the hive metastore to generate a download for. It is required that the table"
                " has both 'awarding_toptier_agency_abbreviation' and 'fiscal_year' columns."
            ),
            default="rpt.transaction_search",
        )

    def handle(self, *args, **options):
        self.validate_options(options)
        num_threads = options["num_threads"]
        download_category_list = []
        logger.info("Downloads will be generated for the following combinations:")
        for idx, option_pair in enumerate(itertools.product(options["agencies"], options["fiscal_years"])):
            temp_download_category = DownloadCategory(idx, options["table_name"], *option_pair)
            download_category_list.append(temp_download_category)
            logger.info(f"\t{temp_download_category}")

        with Timer(f"Starting generation of {len(download_category_list)} downloads with {num_threads} thread(s)"):
            self.spark, spark_created_by_command = self.setup_spark_session(options["schedular"])

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                download_futures = {
                    executor.submit(self.generate_download, download_category): download_category
                    for download_category in download_category_list
                }

        for future in concurrent.futures.as_completed(download_futures):
            download_category = download_futures[future]
            with Timer(f"[{download_category.download_unique_id}] Removing S3 objects for '{download_category}'"):
                try:
                    download_metadata = future.result()
                    objects_to_delete = download_metadata.s3_objects
                    logger.info(
                        f"[{download_category.download_unique_id}] Attempting to delete {len(objects_to_delete)} S3 objects"
                    )
                    deleted_objects = delete_s3_objects(
                        settings.BULK_DOWNLOAD_S3_BUCKET_NAME, key_list=objects_to_delete
                    )
                    logger.info(f"[{download_category.download_unique_id}] Deleted {len(deleted_objects)} S3 objects")
                except Exception:
                    logger.exception(f"Error occurred while generating download for '{download_category}'")
                    raise

        if spark_created_by_command:
            self.spark.stop()

    @staticmethod
    def validate_options(options: dict) -> None:
        valid_fiscal_years = {year for year in range(2008, datetime.now().year + 1)}
        valid_agency_abbreviations = set(ToptierAgency.objects.values_list("abbreviation", flat=True))

        fiscal_years = {int(val) for val in options["fiscal_years"].split(",")}
        invalid_fiscal_years = fiscal_years - valid_fiscal_years
        if invalid_fiscal_years:
            msg = (
                f"The following fiscal years are invalid: {invalid_fiscal_years}."
                " Please provide fiscal years from FY 2008 through the current fiscal year."
            )
            logger.error(msg)
            raise ValueError(msg)
        else:
            options["fiscal_years"] = fiscal_years

        agency_abbreviations = {val.upper() for val in options["agencies"].split(",")}
        invalid_agency_abbreviations = agency_abbreviations - valid_agency_abbreviations
        if invalid_agency_abbreviations:
            msg = (
                f"The following agency abbreviations are invalid: {invalid_agency_abbreviations}."
                " Please provide valid agency abbreviations."
            )
            raise ValueError(msg)
        else:
            options["agencies"] = agency_abbreviations

    @staticmethod
    def setup_spark_session(scheduler_type: SchedulerType) -> tuple[SparkSession, bool]:
        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            extra_conf = {
                **DEFAULT_EXTRA_CONF,
                "spark.scheduler.mode": scheduler_type,
            }
            spark = configure_spark_session(**extra_conf, spark_context=spark)
        return spark, spark_created_by_command

    @staticmethod
    def cleanup(path_list: list[Path | str]) -> None:
        for path in path_list:
            if isinstance(path, str):
                path = Path(path)
            logger.info(f"Removing {path}")
            path.unlink()

    @staticmethod
    def cast_arrays_to_string(df: DataFrame) -> DataFrame:
        array_types = [ArrayType(StringType()), ArrayType(IntegerType())]
        fields_to_convert = [field for field in df.schema if field.dataType in array_types]
        df = df.withColumns({field.name: df[field.name].cast(StringType()) for field in fields_to_convert})
        return df

    def generate_download(self, download_category: DownloadCategory) -> CSVDownloadMetadata:
        with Timer(f"Generating download for {download_category}"):
            spark_to_csv_strategy = SparkToCSVStrategy(logger)
            working_dir_path = Path(settings.CSV_LOCAL_PATH)
            if not working_dir_path.exists():
                working_dir_path.mkdir()
            download_file_name = (
                f"MONTHLY_DOWNLOAD_TEST_{download_category.agency_abbreviation}_{download_category.fiscal_year}"
            )
            download_df = self.cast_arrays_to_string(self.spark.table(download_category.table_name))
            download_df = download_df.filter(
                (download_df.awarding_toptier_agency_abbreviation == download_category.agency_abbreviation)
                & (download_df.fiscal_year == download_category.fiscal_year)
            )
            zip_file_path = working_dir_path / f"{download_file_name}.zip"
            download_metadata = spark_to_csv_strategy.download_to_csv(
                source_sql=None,
                destination_path=working_dir_path / download_file_name,
                destination_file_name=download_file_name,
                working_dir_path=working_dir_path,
                download_zip_path=zip_file_path,
                source_df=download_df,
            )
            logger.info(f"[{download_category.download_unique_id}] Uploading zip file to S3 for {download_category}")
            upload_download_file_to_s3(zip_file_path)
            logger.info(
                f"[{download_category.download_unique_id}] Download contains {download_metadata.number_of_columns}"
                " columns and {download_metadata.number_of_rows} rows"
            )
            self.cleanup(download_metadata.filepaths)

        return download_metadata
