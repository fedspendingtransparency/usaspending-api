import json
import os
import re
import time
import traceback
from logging import Logger
from typing import Optional, Dict, Tuple, Type

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from pyspark.sql import SparkSession, DataFrame

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.dict_helpers import order_nested_object
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_jvm_logger,
    get_usas_jdbc_url,
)
from usaspending_api.download.filestreaming.download_generation import EXCEL_ROW_LIMIT, build_data_file_name
from usaspending_api.download.filestreaming.download_source import DownloadSource
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.management.commands.delta_downloads.award_financial import federal_account
from usaspending_api.download.download_utils import create_unique_filename
from usaspending_api.download.lookups import JOB_STATUS_DICT, FILE_FORMATS, VALUE_MAPPINGS
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.request_validations import AccountDownloadValidator, DownloadValidatorBase

DOWNLOAD_SPEC = {
    "award_financial": {
        "federal_account": {
            # "query": federal_account.TEST_QUERY,
            "query": federal_account.DOWNLOAD_QUERY,
            "validator_type": AccountDownloadValidator,
        }
    }
}


class Command(BaseCommand):

    help = ...

    # TODO: This should be removed prior to any production use
    file_prefix = "SPARK_TEST_"

    # Values defined in the handler
    download_job: DownloadJob
    download_level: Optional[str]
    download_query: str
    download_source: DownloadSource
    download_spec: Dict
    download_type: str
    download_validator_type: Type[DownloadValidatorBase]
    file_format_spec: Dict
    jdbc_properties: Dict
    jdbc_url: str
    logger: Logger
    spark: SparkSession
    temp_dir: str

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
        parser.add_argument("--file-format", type=str, required=False, choices=list(FILE_FORMATS), default="csv")

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

        # Setup Logger
        self.logger = get_jvm_logger(self.spark, __name__)

        # Resolve Parameters
        self.download_type = options["download_type"]
        self.download_level = options["download_level"]

        if self.download_level not in DOWNLOAD_SPEC[self.download_type].keys():
            raise ValueError(
                f'Provided download level of "{self.download_level}" is not supported '
                f'for download type of "{self.download_type}".'
            )

        download_spec = DOWNLOAD_SPEC[self.download_type][self.download_level]
        self.file_format_spec = FILE_FORMATS[options["file_format"]]
        self.download_query = download_spec["query"]
        self.download_validator_type = download_spec["validator_type"]
        self.jdbc_properties = get_jdbc_connection_properties()
        self.jdbc_url = get_usas_jdbc_url()

        create_ref_temp_views(self.spark)

        self.download_job, self.download_source = self.create_download_job()
        self.process_download()

        if spark_created_by_command:
            self.spark.stop()

    @cached_property
    def json_request(self) -> Dict:
        request_data = {
            "account_level": "federal_account",
            "download_types": ["award_financial"],
            "file_format": "csv",
            "filters": {
                "agency": "all",
                "budget_function": "all",
                "budget_subfunction": "all",
                "federal_account": "all",
                "fy": 2021,
                "period": 12,
                "submission_types": ["award_financial"],
            },
            "request_type": "account",
        }
        validator = self.download_validator_type(request_data)
        processed_request = order_nested_object(validator.json_request)

        return processed_request

    @cached_property
    def json_request_string(self) -> str:
        return json.dumps(self.json_request)

    def create_download_job(self) -> Tuple[DownloadJob, DownloadSource]:
        self.logger.info(f"Creating Download Job for {self.download_type} -> {self.download_level}")

        final_output_zip_name = f"{self.file_prefix}{create_unique_filename(self.json_request)}"
        download_job_ready_status = JOB_STATUS_DICT["ready"]

        # Create a download_job object for use by the application
        download_job = DownloadJob.objects.create(
            job_status_id=download_job_ready_status,
            file_name=final_output_zip_name,
            json_request=self.json_request_string,
        )

        # TODO: This should be updated to be more dynamic to the download type
        download_source = DownloadSource(
            VALUE_MAPPINGS[self.download_type]["table_name"],
            self.download_level,
            self.download_type,
            self.json_request.get("agency", "all"),
            # TODO: Is this necessary for Spark downloads? It was originally added to File C downloads for performance.
            extra_file_type="",
        )
        download_source.file_name = build_data_file_name(download_source, download_job, piid=None, assistance_id=None)

        return download_job, download_source

    def process_download(self):
        self.start_download()
        temp_dir = self.create_temp_dir()
        try:
            download_df = self.build_download_dataframe()
            self.create_temp_files(temp_dir, download_df)
            zip_file_path = self.zip_download(temp_dir)

            # TODO: Need to add writing to S3

            self.download_job.file_size = os.stat(zip_file_path).st_size
        except InvalidParameterException as e:
            exc_msg = "InvalidParameterException was raised while attempting to process the DownloadJob"
            self.fail_download(exc_msg, e)
            raise
        except Exception as e:
            exc_msg = "An exception was raised while attempting to process the DownloadJob"
            self.fail_download(exc_msg, e)
            raise
        # finally:
        # shutil.rmtree(temp_dir, ignore_errors=True)

        self.finish_download()

    def create_temp_files(self, dir_path: str, df: DataFrame) -> None:
        self.logger.info(f"Starting to create temp files for DownloadJob {self.download_job.download_job_id}")
        start_time = time.perf_counter()
        delimiter = self.file_format_spec["delimiter"]
        df.write.csv(
            f"{dir_path}/{self.download_type}/{self.download_level}", header=True, mode="overwrite", sep=delimiter
        )
        self.logger.info(f"Creating temp files took {time.perf_counter() - start_time:.4f}s")

    def zip_download(self, dir_path: str) -> str:
        self.logger.info(f"Starting to create zipfile for DownloadJob {self.download_job.download_job_id}")
        start_time = time.perf_counter()
        file_pattern = re.compile(r"^(.*)part-.*\.csv$")

        file_paths = [
            f"{root}/{file_name}"
            for root, _, file_names in list(os.walk(dir_path))[1:]
            for file_name in file_names
            if file_pattern.search(file_name)
        ]
        renamed_file_paths = []

        final_file_name = self.download_source.file_name
        final_file_extension = self.file_format_spec["extension"]

        for idx, file_path in enumerate(file_paths, 1):
            temp_dir_path = file_pattern.search(file_path).group(1)
            new_file_name = f"{temp_dir_path}{final_file_name}_{idx}.{final_file_extension}"
            os.rename(file_path, new_file_name)
            renamed_file_paths.append(new_file_name)

        zip_file_path = f"{dir_path}/{self.download_job.file_name}"
        append_files_to_zip_file(renamed_file_paths, zip_file_path)

        self.logger.info(f"Creating zipfile took {time.perf_counter() - start_time:.4f}s")

        return zip_file_path

    def build_download_dataframe(self) -> DataFrame:
        start_time = time.perf_counter()
        self.logger.info(f"Starting to build DataFrame(s) for DownloadJob {self.download_job.download_job_id}")

        original_df = self.spark.sql(self.download_query)

        num_rows = original_df.count()
        num_partitions = ((num_rows - 1) // EXCEL_ROW_LIMIT) + 1

        self.download_job.number_of_rows = (self.download_job.number_of_rows or 0) + num_rows
        self.download_job.number_of_columns = (self.download_job.number_of_columns or 0) + len(original_df.columns)

        partition_df = original_df.repartition(num_partitions)

        self.logger.info(f"Building DataFrames took {time.perf_counter() - start_time:.4f}s")

        return partition_df

    def start_download(self) -> None:
        self.download_job.job_status_id = JOB_STATUS_DICT["running"]
        self.download_job.save()
        self.logger.info(f"Starting DownloadJob {self.download_job.download_job_id}")

    def fail_download(self, msg: str, e: Optional[Exception] = None) -> None:
        log_dict = {
            "download_job_id": self.download_job.download_job_id,
            "file_name": self.download_job.file_name,
            "message": msg,
            "json_request": self.json_request,
        }

        if e:
            stack_trace = "".join(traceback.format_exception(type(e), value=e, tb=e.__traceback__))
            self.download_job.error_message = f"{msg}:\n{stack_trace}"
            self.logger.exception(log_dict)
        else:
            self.download_job.error_message = msg
            self.logger.error(log_dict)

        self.download_job.job_status_id = JOB_STATUS_DICT["failed"]
        self.download_job.save()

    def finish_download(self) -> None:
        self.download_job.job_status_id = JOB_STATUS_DICT["finished"]
        self.download_job.save()
        self.logger.info(f"Finished processing DownloadJob {self.download_job.download_job_id}")

    def create_temp_dir(self) -> str:
        if settings.IS_LOCAL:
            dir_prefix = settings.CSV_LOCAL_PATH
        else:
            # TODO: This should be moved into settings.py when moved to production
            dir_prefix = f"/dbfs/tmp/"
        dir_path = dir_prefix + self.download_job.file_name.replace(".zip", "")
        os.makedirs(dir_path)
        self.logger.info(f"Created temporary directory at {dir_path}")
        return dir_path
