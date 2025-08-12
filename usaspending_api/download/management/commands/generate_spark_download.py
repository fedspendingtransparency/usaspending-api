import json
import logging
import os
import traceback
from pathlib import Path
from typing import Optional, Union

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
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.common.tracing import SubprocessTrace
from usaspending_api.download.lookups import JOB_STATUS_DICT, FILE_FORMATS
from usaspending_api.download.management.commands.delta_downloads.builders import (
    FederalAccountDownloadDataFrameBuilder,
    TreasuryAccountDownloadDataFrameBuilder,
)
from usaspending_api.download.management.commands.delta_downloads.filters import AccountDownloadFilter
from usaspending_api.download.models import DownloadJob

JOB_TYPE = "USAspendingSparkDownloader"

logger = logging.getLogger(__name__)

dataframe_builders = {
    "federal_account": FederalAccountDownloadDataFrameBuilder,
    "treasury_account": TreasuryAccountDownloadDataFrameBuilder,
}


class Command(BaseCommand):

    help = "Generate a download zip file based on the provided type and level."

    download_job: DownloadJob
    request_type: str
    file_prefix: str
    jdbc_properties: dict
    jdbc_url: str
    should_cleanup: bool
    spark: SparkSession
    working_dir_path: Path

    def add_arguments(self, parser):
        parser.add_argument("--download-job-id", type=int, required=True)
        parser.add_argument("--file-format", type=str, required=False, choices=list(FILE_FORMATS), default="csv")
        parser.add_argument("--file-prefix", type=str, required=False, default="")
        parser.add_argument("--skip-local-cleanup", action="store_true")

    def handle(self, *args, **options):
        self.spark, spark_created_by_command = self.setup_spark_session()
        self.file_prefix = options["file_prefix"]
        self.should_cleanup = not options["skip_local_cleanup"]
        self.download_job = self.get_download_job(options["download_job_id"])
        self.request_type = json.loads(self.download_job.json_request)["request_type"]
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
        if download_job.job_status_id != JOB_STATUS_DICT["ready"]:
            with SubprocessTrace(
                name=f"job.{JOB_TYPE}.download.download_job_id-{download_job_id}",
                kind=SpanKind.INTERNAL,
                service="spark",
            ) as get_download_job_error:
                get_download_job_error.set_attributes(
                    {
                        "download_job_id": download_job_id,
                        "download_job_status": JOB_STATUS_DICT[download_job.job_status_id],
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
            zip_file_path = self.working_dir_path / f"{self.download_name}.zip"
            download_request = json.loads(self.download_job.json_request)
            df_builder = dataframe_builders[download_request["account_level"]]
            account_download_filter = AccountDownloadFilter(**download_request["filters"])
            source_dfs = df_builder(spark=self.spark, account_download_filter=account_download_filter).source_dfs
            csvs_metadata = [
                spark_to_csv_strategy.download_to_csv(
                    source_sql=None,
                    destination_path=self.working_dir_path / self.download_name,
                    destination_file_name=self.download_name,
                    working_dir_path=self.working_dir_path,
                    download_zip_path=zip_file_path,
                    source_df=source_df,
                )
                for source_df in source_dfs
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
