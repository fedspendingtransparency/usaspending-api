import json
import logging
import os
import traceback
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, Optional, TypeVar, Union

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from opentelemetry.trace import SpanKind
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.download_csv_strategies import SparkToCSVStrategy
from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.common.logging import configure_logging
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.common.tracing import SubprocessTrace
from usaspending_api.download.delta_downloads.abstract_downloads.base_download import AbstractDownload
from usaspending_api.download.delta_downloads.account_balances import AccountBalancesDownloadFactory
from usaspending_api.download.delta_downloads.award_financial import AwardFinancialDownloadFactory
from usaspending_api.download.delta_downloads.object_class_program_activity import (
    ObjectClassProgramActivityDownloadFactory,
)
from usaspending_api.download.delta_downloads.filters.account_filters import AccountDownloadFilters
from usaspending_api.download.lookups import FILE_FORMATS, JOB_STATUS_DICT, JOB_STATUS_DICT_BY_ID
from usaspending_api.download.models import DownloadJob
from usaspending_api.settings import TRACE_ENV

JOB_TYPE = "USAspendingSparkDownloader"


logger = logging.getLogger(__name__)

Download = TypeVar("Download", bound=AbstractDownload)


class DownloadType(str, Enum):
    ACCOUNT = "account"


@dataclass
class DownloadRequest:
    download_list: list[Download]
    file_delimiter: str
    file_extension: Literal["csv", "tsv", "txt"]
    type: DownloadType


class Command(BaseCommand):

    help = "Generate a download zip file based on the provided type and level."

    download_job: DownloadJob
    download_json_request: dict
    request_type: str
    should_cleanup: bool
    spark: SparkSession
    working_dir_path: Path

    def add_arguments(self, parser):
        parser.add_argument("--download-job-id", type=int, required=True)
        parser.add_argument("--skip-local-cleanup", action="store_true")

    def handle(self, *args, **options):
        configure_logging(service_name="usaspending-downloader-" + TRACE_ENV)
        self.spark, spark_created_by_command = self.setup_spark_session()
        self.should_cleanup = not options["skip_local_cleanup"]
        self.download_job = self.get_download_job(options["download_job_id"])
        self.download_json_request = json.loads(self.download_job.json_request)
        self.request_type = self.download_json_request["request_type"]
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
    def download_zip_file_name(self) -> str:
        return self.download_job.file_name.replace(".zip", "")

    @staticmethod
    def get_download_job(download_job_id) -> DownloadJob:
        download_job = DownloadJob.objects.get(download_job_id=download_job_id)
        if download_job.job_status_id not in (JOB_STATUS_DICT["ready"], JOB_STATUS_DICT["failed"]):
            # Handles both new downloads and retries of failed downloads
            with SubprocessTrace(
                name=f"job.{JOB_TYPE}.download.download_job_id-{download_job_id}",
                kind=SpanKind.INTERNAL,
                service="spark",
            ) as get_download_job_error:
                get_download_job_error.set_attributes(
                    {
                        "download_job_id": download_job_id,
                        "download_job_status": JOB_STATUS_DICT_BY_ID[download_job.job_status_id],
                        "message": f"Download Job {download_job_id} is not ready,",
                    }
                )
            raise InvalidParameterException(f"Download Job {download_job_id} is not ready.")
        return download_job

    def process_download(self):
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.download.spark.{self.request_type}",
            kind=SpanKind.INTERNAL,
            service="spark",
        ) as main_trace:
            main_trace.set_attributes(
                {
                    "service": "spark",
                    "span_type": "Internal",
                    "job_type": str(JOB_TYPE),
                    "message": f"Processing spark account download.",
                    # download job details
                    "download_job_id": str(self.download_job.download_job_id),
                    "download_job_status": str(self.download_job.job_status.name),
                    "download_file_name": str(self.download_job.file_name),
                    "download_file_size": self.download_job.file_size if self.download_job.file_size is not None else 0,
                    "number_of_rows": (
                        self.download_job.number_of_rows if self.download_job.number_of_rows is not None else 0
                    ),
                    "number_of_columns": (
                        self.download_job.number_of_columns if self.download_job.number_of_columns is not None else 0
                    ),
                    "error_message": self.download_job.error_message if self.download_job.error_message else "",
                    "monthly_download": str(self.download_job.monthly_download),
                    "json_request": str(self.download_job.json_request) if self.download_job.json_request else "",
                    "file_name": str(self.download_job.file_name),
                }
            )

        self.start_download()
        files_to_cleanup = []
        try:
            spark_to_csv_strategy = SparkToCSVStrategy(logger)
            zip_file_path = self.working_dir_path / f"{self.download_zip_file_name}.zip"
            download_request = self.get_download_request()
            csvs_metadata = [
                spark_to_csv_strategy.download_to_csv(
                    source_sql=None,
                    destination_path=self.working_dir_path / download.file_name,
                    destination_file_name=download.file_name,
                    working_dir_path=self.working_dir_path,
                    download_zip_path=zip_file_path,
                    source_df=download.dataframe,
                    delimiter=download_request.file_delimiter,
                    file_format=download_request.file_extension,
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
            if not settings.IS_LOCAL:
                with SubprocessTrace(
                    name=f"job.{JOB_TYPE}.download.s3",
                    kind=SpanKind.INTERNAL,
                    service="spark",
                ) as span:
                    span.set_attributes(
                        {
                            "service": "spark",
                            "span_type": "Internal",
                            "resource": f"s3://{settings.BULK_DOWNLOAD_S3_BUCKET_NAME}",
                            "message": "Push file to S3 bucket, if not local",
                        }
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

    def get_download_request(self) -> DownloadRequest:
        download_factories = {
            "account_balances": AccountBalancesDownloadFactory,
            "award_financial": AwardFinancialDownloadFactory,
            "object_class_program_activity": ObjectClassProgramActivityDownloadFactory,
        }
        match self.request_type:
            case DownloadType.ACCOUNT:
                filters = AccountDownloadFilters(**self.download_json_request["filters"])
                account_level = self.download_json_request.get("account_level")
                download_list: list[Download] = [
                    download_factories[submission_type](self.spark, filters).get_download(account_level)
                    for submission_type in filters.submission_types
                ]
                download_type = DownloadType.ACCOUNT
            case _:
                raise NotImplementedError(
                    f"Download request type '{self.request_type}' is not implemented for Spark downloads"
                )
        file_format = FILE_FORMATS[self.download_json_request.get("file_format", "csv")]
        return DownloadRequest(
            download_list=download_list,
            file_delimiter=file_format["delimiter"],
            file_extension=file_format["extension"],
            type=download_type,
        )

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
        with SubprocessTrace(
            name=f"job.{JOB_TYPE}.download.spark.{self.request_type}",
            kind=SpanKind.INTERNAL,
            service="spark",
        ) as error_span:
            error_span.set_attributes(
                {
                    "service": "spark",
                    "span_type": "Internal",
                    "message": self.download_job.error_message,
                    "error": str(e),
                }
            )
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
