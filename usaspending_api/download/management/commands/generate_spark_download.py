import json
import logging
import os
import traceback
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.download_csv_strategies import SparkToCSVStrategy
from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.download.delta_downloads.abstract_downloads.base_download import AbstractDownload
from usaspending_api.download.delta_downloads.account_balances import AccountBalancesDownloadFactory
from usaspending_api.download.delta_downloads.award_financial import AwardFinancialDownloadFactory
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob

if TYPE_CHECKING:
    from typing import Optional, TypeVar, Union
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

Download = TypeVar("Download", bound=AbstractDownload)


class DownloadType(str, Enum):
    ACCOUNT = "account"


@dataclass
class DownloadRequest:
    download_list: list[Download]
    file_format: Literal["csv", "pstxt", "tsv"]
    type: DownloadType


class Command(BaseCommand):

    help = "Generate a download zip file based on the provided type and level."

    download_job: DownloadJob
    file_prefix: str
    jdbc_properties: dict
    jdbc_url: str
    should_cleanup: bool
    spark: SparkSession
    working_dir_path: Path

    def add_arguments(self, parser):
        parser.add_argument("--download-job-id", type=int, required=True)
        parser.add_argument("--skip-local-cleanup", action="store_true")

    def handle(self, *args, **options):
        self.spark, spark_created_by_command = self.setup_spark_session()
        self.should_cleanup = not options["skip_local_cleanup"]
        self.download_job = self.get_download_job(options["download_job_id"])
        self.working_dir_path = Path(settings.CSV_LOCAL_PATH)
        if not self.working_dir_path.exists():
            self.working_dir_path.mkdir()
        create_ref_temp_views(self.spark)
        self.process_download()
        if spark_created_by_command:
            self.spark.stop()

    @staticmethod
    def setup_spark_session() -> tuple[SparkSession, bool]:
        spark = get_active_spark_session()
        spark_created_by_command = False
        if not spark:
            spark_created_by_command = True
            spark = configure_spark_session(**DEFAULT_EXTRA_CONF, spark_context=spark)
        return spark, spark_created_by_command

    @cached_property
    def download_name(self) -> str:
        return self.download_job.file_name.replace(".zip", "")

    @staticmethod
    def get_download_job(download_job_id) -> DownloadJob:
        download_job = DownloadJob.objects.get(download_job_id=download_job_id)
        if download_job.job_status_id not in (JOB_STATUS_DICT["ready"], JOB_STATUS_DICT["failed"]):
            # Handles both new downloads and retries of failed downloads
            raise InvalidParameterException(f"Download Job {download_job_id} is not ready.")
        return download_job

    def process_download(self):
        self.start_download()
        files_to_cleanup = []
        try:
            spark_to_csv_strategy = SparkToCSVStrategy(logger)
            zip_file_path = self.working_dir_path / f"{self.download_name}.zip"
            download_request = self.get_download_requests()
            csvs_metadata = [
                spark_to_csv_strategy.download_to_csv(
                    source_sql=None,
                    destination_path=self.working_dir_path / self.download_name,
                    destination_file_name=download.file_name,
                    working_dir_path=self.working_dir_path,
                    download_zip_path=zip_file_path,
                    source_df=download.dataframe,
                )
                for download in download_request.download_list
            ]
            for csv_metadata in csvs_metadata:
                files_to_cleanup.extend(csv_metadata.filepaths)
            self.download_job.file_size = os.stat(zip_file_path).st_size
            self.download_job.number_of_rows = sum([csv_metadata.number_of_rows for csv_metadata in csvs_metadata])
            self.download_job.number_of_columns = sum(
                [csv_metadata.number_of_columns for csv_metadata in csvs_metadata]
            )
            upload_download_file_to_s3(zip_file_path)
        except InvalidParameterException as e:
            exc_msg = "InvalidParameterException was raised while attempting to process the DownloadJob"
            self.fail_download(exc_msg, e)
            raise
        except Exception as e:
            exc_msg = "An exception was raised while attempting to process the DownloadJob"
            self.fail_download(exc_msg, e)
            raise
        finally:
            if self.should_cleanup:
                self.cleanup(files_to_cleanup)
        self.finish_download()

    def get_download_requests(self) -> DownloadRequest:
        download_factories = {
            "account_balances": AccountBalancesDownloadFactory,
            "award_financial": AwardFinancialDownloadFactory,
        }
        download_json = json.loads(self.download_job.json_request)
        request_type = download_json.get("request_type")
        file_format = download_json.get("file_format")
        match request_type:
            case DownloadType.ACCOUNT:
                filters = AccountDownloadFilters(**download_json["filters"])
                factories = [
                    download_factories[submission_type](self.spark, filters)
                    for submission_type in filters.submission_types
                ]
                download_request = DownloadRequest(
                    download_list=[
                        getattr(factory, f"create_{download_json['account_level']}_download")() for factory in factories
                    ],
                    file_format=file_format,
                    type=DownloadType.ACCOUNT,
                )
            case _:
                raise NotImplementedError(
                    f"Download request type '{request_type}' is not implemented for Spark downloads"
                )
        return download_request

    def start_download(self) -> None:
        self.download_job.job_status_id = JOB_STATUS_DICT["running"]
        self.download_job.save()
        logger.info(f"Starting DownloadJob {self.download_job.download_job_id}")

    def fail_download(self, msg: str, e: Optional[Exception] = None) -> None:
        if e:
            stack_trace = "".join(traceback.format_exception(type(e), value=e, tb=e.__traceback__))
            self.download_job.error_message = f"{msg}:\n{stack_trace}"
        else:
            self.download_job.error_message = msg
        logger.error(msg)
        self.download_job.job_status_id = JOB_STATUS_DICT["failed"]
        self.download_job.save()

    def finish_download(self) -> None:
        self.download_job.job_status_id = JOB_STATUS_DICT["finished"]
        self.download_job.save()
        logger.info(f"Finished processing DownloadJob {self.download_job.download_job_id}")

    def cleanup(self, path_list: list[Union[Path, str]]) -> None:
        for path in path_list:
            if isinstance(path, str):
                path = Path(path)
            logger.info(f"Removing {path}")
            path.unlink()
