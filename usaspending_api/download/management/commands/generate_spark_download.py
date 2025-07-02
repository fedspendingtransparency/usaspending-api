import json
import logging
import os
import traceback
from pathlib import Path
from typing import Optional, Dict, Tuple, Type, List, Union

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.download_csv_strategies import SparkToCSVStrategy
from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
)
from usaspending_api.common.spark.configs import DEFAULT_EXTRA_CONF
from usaspending_api.download.filestreaming.download_generation import build_data_file_name
from usaspending_api.download.filestreaming.download_source import DownloadSource
from usaspending_api.download.lookups import JOB_STATUS_DICT, FILE_FORMATS, VALUE_MAPPINGS
from usaspending_api.download.management.commands.delta_downloads.award_financial.builders import (
    AccountDownloadDataFrameBuilder,
)
from usaspending_api.download.management.commands.delta_downloads.award_financial.filters import AccountDownloadFilter
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.request_validations import AccountDownloadValidator, DownloadValidatorBase

logger = logging.getLogger(__name__)

DOWNLOAD_SPEC = {
    "award_financial": {
        "federal_account": {
            "df_builder": AccountDownloadDataFrameBuilder,
            "validator_type": AccountDownloadValidator,
        }
    }
}


class Command(BaseCommand):

    help = "Generate a download zip file based on the provided type and level."

    download_job_id: int
    download_job: DownloadJob
    download_level: str
    download_query: str
    download_source: DownloadSource
    download_spec: Dict
    download_type: str
    download_validator_type: Type[DownloadValidatorBase]
    file_format_spec: Dict
    file_prefix: str
    jdbc_properties: Dict
    jdbc_url: str
    should_cleanup: bool
    spark: SparkSession
    working_dir_path: Path

    def add_arguments(self, parser):
        parser.add_argument("--download-type", type=str, required=True, choices=list(DOWNLOAD_SPEC))
        parser.add_argument(
            "--download-level",
            type=str,
            required=True,
            choices=set(
                download_level
                for download_level_list in [DOWNLOAD_SPEC[key] for key in DOWNLOAD_SPEC]
                for download_level in download_level_list
            ),
        )
        parser.add_argument("--download-job-id", type=int, required=True)
        parser.add_argument("--file-format", type=str, required=False, choices=list(FILE_FORMATS), default="csv")
        parser.add_argument("--file-prefix", type=str, required=False, default="")
        parser.add_argument("--skip-local-cleanup", action="store_true")

    def handle(self, *args, **options):
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**DEFAULT_EXTRA_CONF, spark_context=self.spark)

        # Resolve Parameters
        self.download_type = options["download_type"]
        self.download_level = options["download_level"]
        self.download_job_id = options["download_job_id"]
        self.file_prefix = options["file_prefix"]
        self.should_cleanup = not options["skip_local_cleanup"]

        if self.download_level not in DOWNLOAD_SPEC[self.download_type].keys():
            raise ValueError(
                f'Provided download level of "{self.download_level}" is not supported '
                f'for download type of "{self.download_type}".'
            )

        download_spec = DOWNLOAD_SPEC[self.download_type][self.download_level]
        self.file_format_spec = FILE_FORMATS[options["file_format"]]
        self.df_builder = download_spec["df_builder"]
        self.download_validator_type = download_spec["validator_type"]
        self.jdbc_properties = get_jdbc_connection_properties()
        self.jdbc_url = get_usas_jdbc_url()

        self.working_dir_path = Path(settings.CSV_LOCAL_PATH)
        if not self.working_dir_path.exists():
            self.working_dir_path.mkdir()

        create_ref_temp_views(self.spark)

        self.download_job, self.download_source = self.get_download_job()
        self.process_download()

        if spark_created_by_command:
            self.spark.stop()

    @cached_property
    def download_name(self) -> str:
        return self.download_job.file_name.replace(".zip", "")

    def get_download_job(self) -> Tuple[DownloadJob, DownloadSource]:
        download_job = DownloadJob.objects.get(download_job_id=self.download_job_id)
        if download_job.job_status_id != JOB_STATUS_DICT["ready"]:
            raise InvalidParameterException(f"Download Job {self.download_job_id} is not ready.")
        json_request = json.loads(download_job.json_request)
        download_source = DownloadSource(
            VALUE_MAPPINGS[self.download_type]["table_name"],
            self.download_level,
            self.download_type,
            json_request.get("agency", "all"),
        )
        download_source.file_name = build_data_file_name(download_source, download_job, piid=None, assistance_id=None)

        return download_job, download_source

    def process_download(self):
        self.start_download()
        files_to_cleanup = []
        try:
            spark_to_csv_strategy = SparkToCSVStrategy(logger)
            zip_file_path = self.working_dir_path / f"{self.download_name}.zip"
            download_request = json.loads(self.download_job.json_request)
            account_download_filter = AccountDownloadFilter(**download_request["filters"])
            source_df = self.df_builder(spark=self.spark, account_download_filter=account_download_filter).source_df
            csv_metadata = spark_to_csv_strategy.download_to_csv(
                source_sql=None,
                destination_path=self.working_dir_path / self.download_name,
                destination_file_name=self.download_name,
                working_dir_path=self.working_dir_path,
                download_zip_path=zip_file_path,
                source_df=source_df,
            )
            files_to_cleanup.extend(csv_metadata.filepaths)
            self.download_job.file_size = os.stat(zip_file_path).st_size
            self.download_job.number_of_rows = csv_metadata.number_of_rows
            self.download_job.number_of_columns = csv_metadata.number_of_columns
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
        self.download_job.job_status_id = JOB_STATUS_DICT["failed"]
        self.download_job.save()

    def finish_download(self) -> None:
        self.download_job.job_status_id = JOB_STATUS_DICT["finished"]
        self.download_job.save()
        logger.info(f"Finished processing DownloadJob {self.download_job.download_job_id}")

    def cleanup(self, path_list: List[Union[Path, str]]) -> None:
        for path in path_list:
            if isinstance(path, str):
                path = Path(path)
            logger.info(f"Removing {path}")
            path.unlink()
