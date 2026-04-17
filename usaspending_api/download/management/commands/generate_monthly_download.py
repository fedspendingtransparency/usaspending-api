import concurrent.futures
import itertools
import logging
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import boto3
from django.core.management import BaseCommand, CommandParser
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from usaspending_api import settings
from usaspending_api.common.helpers.download_csv_strategies import CSVDownloadMetadata, SparkToCSVStrategy
from usaspending_api.common.helpers.fiscal_year_helpers import create_fiscal_year_list, generate_fiscal_year
from usaspending_api.common.helpers.s3_helpers import delete_s3_objects, get_s3_bucket, upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.common.helpers.timing_helpers import Timer as BaseTimer
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.config import CONFIG
from usaspending_api.download.delta_downloads.abstract_factories.monthly_download_factory import (
    MonthlyDownload,
)
from usaspending_api.download.delta_downloads.filters.monthly_download_filters import MonthlyDownloadFilters
from usaspending_api.download.delta_downloads.helpers.enums import AwardCategory, MonthlyType
from usaspending_api.download.delta_downloads.transaction_assistance_monthly import (
    TransactionAssistanceMonthlyDownloadFactory,
)
from usaspending_api.download.delta_downloads.transaction_contract_monthly import (
    TransactionContractMonthlyDownloadFactory,
)
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.v2.download_list_agencies import DownloadListAgenciesViewSet

logger = logging.getLogger(__name__)


class DownloadFuture(NamedTuple):
    download: MonthlyDownload
    future: concurrent.futures.Future


class Timer(BaseTimer):
    def __init__(self, message: str | None = None):
        super().__init__(message=message, success_logger=logger.info, failure_logger=logger.error)


class Command(BaseCommand):
    bucket: "boto3.resources.factory.s3.Instance"
    empty_file_dfs: dict[AwardCategory, DataFrame]
    monthly_type = MonthlyType
    spark: SparkSession
    verbosity: int
    working_dir_path = Path(settings.CSV_LOCAL_PATH)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.abbreviation_to_toptier_lookup = {}
        self.toptier_to_abbreviation_lookup = {}
        all_toptier_agencies = DownloadListAgenciesViewSet.get_award_agencies_queryset().values(
            "abbreviation", "toptier_code"
        )
        for toptier_agency in all_toptier_agencies:
            abbreviation, toptier_code = toptier_agency.values()
            self.abbreviation_to_toptier_lookup[abbreviation] = toptier_code
            self.toptier_to_abbreviation_lookup[toptier_code] = abbreviation

        # TODO: Determine what the start date should be; we generate starting in 2007 but only show starting in FY 2008
        self.fiscal_year_choices = create_fiscal_year_list(
            start_year=generate_fiscal_year(datetime.strptime(settings.API_SEARCH_MIN_DATE, "%Y-%m-%d").date()),
        )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "-o",
            "--overwrite",
            dest="overwrite",
            action="store_true",
            default=False,
            help="Generate and upload files regardless if they have already been uploaded that day",
        )
        parser.add_argument(
            "-all",
            "--all-agencies",
            dest="all_agencies",
            action="store_true",
            default=False,
            help="Generates a download for all agencies that are user selectable",
        )
        parser.add_argument(
            "-a",
            "--agencies",
            dest="agencies",
            nargs="+",
            default=[],
            choices=["all", *self.abbreviation_to_toptier_lookup.keys(), *self.toptier_to_abbreviation_lookup.keys()],
            type=str,
            help="Specific toptier agency codes or abbreviations",
        )
        parser.add_argument(
            "-ac",
            "--award-categories",
            dest="award_categories",
            nargs="*",
            default=[category.value for category in AwardCategory],
            choices=[category.value for category in AwardCategory],
            type=str,
            help="Specific award categories",
        )
        parser.add_argument(
            "-fy",
            "--fiscal-years",
            dest="fiscal_years",
            nargs="+",
            default=self.fiscal_year_choices,
            choices=self.fiscal_year_choices,
            type=int,
            help="Specific fiscal years",
        )
        parser.add_argument(
            "-e",
            "--empty-files",
            dest="empty_files",
            nargs="*",
            default=[],
            type=str,
            choices=[category.value for category in AwardCategory],
            help="Which download should include an empty file",
        )
        parser.add_argument(
            "-s--skip-local-cleanup",
            dest="cleanup",
            action="store_true",
            default=False,
            help="Deletes the previous version of the newly generated file after uploading"
            " (only applies if --local is also provided).",
        )
        parser.add_argument(
            "-n--num-threads",
            dest="num_threads",
            default=1,
            type=int,
            help="Number of threads to use when processing monthly downloads",
        )
        parser.add_argument(
            "-m",
            "--monthly-type",
            dest="monthly_type",
            choices=[monthly_type.value for monthly_type in MonthlyType],
            type=str,
            required=True,
            help="Type of monthly download to generate",
        )
        parser.add_argument(
            "-b",
            "--bucket-name",
            dest="bucket_name",
            default=CONFIG.MONTHLY_DOWNLOAD_S3_BUCKET_NAME,
            type=str,
            help="Location where downloads will be stored",
        )

    def handle(self, *args, **options) -> None:
        if options["monthly_type"] == MonthlyType.DELTA:
            raise RuntimeError("This command doesn't yet support monthly delta downloads")

        validated_options = self.validate_agency_options(options)
        self.verbosity = validated_options["verbosity"]

        # This is the setting used by current download process(es). We override with monthly download bucket name
        # to provide more shared functionality between downloads.
        CONFIG.BULK_DOWNLOAD_S3_BUCKET_NAME = validated_options["bucket_name"]

        if not self.working_dir_path.exists():
            self.working_dir_path.mkdir()

        self.monthly_type = MonthlyType(validated_options["monthly_type"])
        self.bucket = get_s3_bucket(CONFIG.BULK_DOWNLOAD_S3_BUCKET_NAME)
        self.spark, spark_created_by_command = self.setup_spark_session()

        self.empty_file_dfs = {
            category: self.get_empty_dataframe(category) for category in validated_options["empty_files"]
        }

        current_date = datetime.strftime(datetime.today(), "%Y%m%d")
        download_prefixes_to_skip = (
            [] if validated_options["overwrite"] else self.get_existing_download_file_prefixes(current_date)
        )

        downloads_to_process = self.get_downloads_to_process(
            current_date,
            download_prefixes_to_skip,
            validated_options["agencies"],
            validated_options["fiscal_years"],
            validated_options["award_categories"],
        )

        download_status = {"success": [], "failed": []}

        with (
            Timer(f"Generating {len(downloads_to_process)} downloads"),
            concurrent.futures.ThreadPoolExecutor(
                max_workers=validated_options["num_threads"]
            ) as generate_download_executor,
        ):
            generate_download_futures = [
                DownloadFuture(
                    download=download,
                    future=generate_download_executor.submit(self.generate_download_files, index, download),
                )
                for index, download in enumerate(downloads_to_process, start=1)
            ]

            # This is left at the default to ensure number of workers are appropriate to the Spark Driver's resources.
            # These steps will be almost entirely Python processes that cannot be handed off to the Spark Executors.
            with concurrent.futures.ThreadPoolExecutor() as finalize_download_executor:
                finalize_download_futures = {
                    finalize_download_executor.submit(self.finalize_download, index, download, future): download
                    for index, (download, future) in enumerate(
                        generate_download_futures, start=1 + len(generate_download_futures)
                    )
                }
                for future in concurrent.futures.as_completed(finalize_download_futures):
                    download = finalize_download_futures[future]
                    try:
                        # We don't use the result, but want to make sure this didn't run into an exception
                        future.result()
                        download_status["success"].append(download)
                    except Exception:
                        # Log the exception, but continue processing; capture failed downloads at the end
                        logger.exception(f"Error occurred while finalizing download: {download.file_names[0]}")
                        download_status["failed"].append(download)

        if self.verbosity > 1:
            logger.info("The following downloads were successfully generated:")
            for download in sorted(download_status["success"], key=lambda d: d.file_name_prefix):
                logger.info(f"\t{download.file_names[0]}")

        if download_status["failed"]:
            logger.error("The following downloads failed to generate:")
            for download in sorted(download_status["failed"], key=lambda d: d.file_name_prefix):
                logger.info(f"\t{download.file_names[0]}")
            raise RuntimeError(
                f"{len(download_status['failed'])} of {len(downloads_to_process)} downloads failed to generate"
                " (see logs for more details)."
            )

        if spark_created_by_command:
            self.spark.stop()

    def validate_agency_options(self, options: dict) -> dict:
        result = deepcopy(options)
        if options["all_agencies"] and len(options["agencies"]) > 0:
            raise ValueError("Only one of the following arguments is allowed: '--all-agencies' or '--agencies'")

        if not options["all_agencies"] and len(options["agencies"]) == 0:
            raise ValueError("One of the following arguments is required: '--all-agencies' or '--agencies'")

        if options["all_agencies"]:
            result["agencies"] = [*self.toptier_to_abbreviation_lookup.keys()]
        else:
            result["agencies"] = [
                val if val in self.toptier_to_abbreviation_lookup.keys() else self.abbreviation_to_toptier_lookup[val]
                for val in result["agencies"]
            ]

        return result

    def get_downloads_to_process(
        self,
        as_of_date: str,
        downloads_prefixes_to_skip: list[str],
        agencies: list[str],
        fiscal_years: list[int],
        award_categories: list[AwardCategory],
    ) -> list[MonthlyDownload]:
        result = []

        award_categories = set(award_categories) | set(self.empty_file_dfs)
        download_combinations = itertools.product(agencies, fiscal_years, award_categories)

        for agency_code, fiscal_year, award_category in download_combinations:
            filter_kwargs = {"as_of_date": as_of_date, "fiscal_year": fiscal_year}
            if agency_code != "all":
                filter_kwargs["awarding_toptier_agency_code"] = agency_code
            download_filters = MonthlyDownloadFilters(**filter_kwargs)
            factory_class = (
                TransactionAssistanceMonthlyDownloadFactory
                if award_category == AwardCategory.ASSISTANCE
                else TransactionContractMonthlyDownloadFactory
            )
            download_factory = factory_class(self.spark, download_filters)
            download = download_factory.get_download(self.monthly_type)

            if download.file_name_prefix in downloads_prefixes_to_skip:
                logger.info(f"Skipping {download.file_names[0]}")
            else:
                result.append(download)

        return result

    def get_existing_download_file_prefixes(self, as_of_date: str) -> list[str]:
        result = []
        for obj in self.bucket.objects.all():
            pattern = rf"^(.*_{self.monthly_type.title})_{as_of_date}.zip$"
            match = re.fullmatch(pattern, obj.key)
            if match:
                result.append(match.group(1))
        return result

    def setup_spark_session(self) -> tuple[SparkSession, bool]:
        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**DEFAULT_EXTRA_CONF, spark_context=spark)
        return spark, spark_created_by_command

    def cleanup(self, path_list: list[Path | str]) -> None:
        for path in path_list:
            if isinstance(path, str):
                path = Path(path)
            if self.verbosity > 1:
                logger.info(f"Removing {path}")
            path.unlink()

    def generate_download_files(self, index: int, monthly_download: MonthlyDownload) -> CSVDownloadMetadata:
        with Timer(self.thread_log_message(index, f"Generate download for {monthly_download.file_names[0]}")):
            df = self.empty_file_dfs.get(monthly_download.category, monthly_download.dataframes[0])
            download_file_name = monthly_download.file_names[0]
            zip_file_path = self.working_dir_path / f"{download_file_name}.zip"
            download_metadata = SparkToCSVStrategy(logger).download_to_csv(
                source_sql=None,
                destination_path=str(self.working_dir_path / download_file_name),
                destination_file_name=download_file_name,
                working_dir_path=str(self.working_dir_path),
                download_zip_path=str(zip_file_path),
                source_df=df,
                spark=self.spark,
                skip_s3_delete=True,
                skip_zip_file=True,
            )
            logger.info(self.thread_log_message(index, f"Download contains {download_metadata.number_of_rows} rows"))
        return download_metadata

    def finalize_download(
        self, index: int, monthly_download: MonthlyDownload, generate_download_future: concurrent.futures.Future
    ) -> CSVDownloadMetadata:
        file_name = monthly_download.file_names[0]
        try:
            download_metadata = generate_download_future.result()
        except Exception:
            logger.exception(
                self.thread_log_message(index, f"Error occurred while generating download for '{file_name}'")
            )
            raise

        with Timer(self.thread_log_message(index, f"Finalizing download for {file_name}")):
            file_paths = download_metadata.filepaths
            object_keys = download_metadata.object_keys
            zip_file_path = self.working_dir_path / f"{file_name}.zip"

            logger.info(self.thread_log_message(index, f"Adding {len(file_paths)} files to zip file"))
            append_files_to_zip_file(download_metadata.filepaths, zip_file_path)

            logger.info(self.thread_log_message(index, "Uploading zip file to S3"))
            upload_download_file_to_s3(zip_file_path)

            logger.info(self.thread_log_message(index, f"Attempting to delete {len(object_keys)} S3 objects"))
            deleted_keys = delete_s3_objects(CONFIG.BULK_DOWNLOAD_S3_BUCKET_NAME, key_list=object_keys)

            if self.verbosity > 1:
                logger.info(self.thread_log_message(index, f"Deleted {len(deleted_keys)} S3 objects"))
                for key in deleted_keys:
                    logger.info(self.thread_log_message(index, f"Deleted key: {key}"))

            logger.info(self.thread_log_message(index, "Cleaning up"))
            self.cleanup(download_metadata.filepaths)

            return download_metadata

    @staticmethod
    def thread_log_message(index: int, message: str) -> str:
        """Convenience method to consistently format logs across threads"""
        return f"Thread #{index:03d} -- {message}"

    def get_empty_dataframe(self, category: AwardCategory) -> DataFrame:
        """
        Generate temporary download factory, filters, and downloads for the purpose of creating a blank file
        that matches the exact structure of the downloads generated with data.
        """
        if category == AwardCategory.ASSISTANCE:
            factory_class = TransactionAssistanceMonthlyDownloadFactory
        else:
            factory_class = TransactionContractMonthlyDownloadFactory

        # Generic filters for the purpose of bypassing the Pydantic checks
        monthly_filters = MonthlyDownloadFilters(fiscal_year=generate_fiscal_year(datetime.today()))

        download = factory_class(self.spark, monthly_filters).get_download(self.monthly_type)

        # Create a schema using StringTypes for simplicity as the dataframe won't contain any data
        schema = StructType([StructField(col._jc.named().name(), StringType()) for col in download.select_cols])

        return self.spark.createDataFrame([], schema=schema)
