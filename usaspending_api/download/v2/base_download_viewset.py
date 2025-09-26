import json
from datetime import datetime, timezone
from typing import List, Optional, Type

from django.conf import settings
from django.db.models import Max
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.broker.lookups import EXTERNAL_DATA_TYPE_DICT
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.experimental_api_flags import is_experimental_download_api
from usaspending_api.common.helpers.dict_helpers import order_nested_object
from usaspending_api.common.spark.jobs import DatabricksStrategy, LocalStrategy, SparkJobs
from usaspending_api.common.sqs.sqs_handler import get_sqs_queue
from usaspending_api.download.download_utils import create_unique_filename, log_new_download_job
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.v2.request_validations import DownloadValidatorBase
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseDownloadViewSet(APIView):
    def post(
        self,
        request: Request,
        validator_type: Type[DownloadValidatorBase],
        origination: Optional[str] = None,
    ):
        validator = validator_type(request.data)
        json_request = validator.json_request

        # Check if download is pre-generated
        pre_generated_download = json_request.pop("pre_generated_download", None)
        if pre_generated_download:
            download_job = (
                DownloadJob.objects.filter(
                    file_name__startswith=pre_generated_download["name_match"],
                    json_request__contains=pre_generated_download["request_match"],
                    error_message__isnull=True,
                )
                .order_by("-update_date")
                .first()
            )
            return self.build_download_response(download_job)

        # Check if the same request has been called today
        sorted_json_request = order_nested_object(validator.json_request)
        ordered_json_request = json.dumps(sorted_json_request)
        cached_download = self._get_cached_download(ordered_json_request, json_request.get("download_types", []))

        if cached_download and not settings.IS_LOCAL:
            # By returning the cached files, there should be no duplicates on a daily basis
            write_to_log(message=f"Generating file from cached download job ID: {cached_download.download_job_id}")
            return self.build_download_response(cached_download)

        final_output_zip_name = create_unique_filename(json_request, origination=origination)
        download_job = DownloadJob.objects.create(
            job_status_id=JOB_STATUS_DICT["ready"], file_name=final_output_zip_name, json_request=json.dumps(json_request)
        )

        log_new_download_job(request, download_job)
        self.process_request(download_job, request, json_request)

        return self.build_download_response(download_job)

    def process_request(self, download_job: DownloadJob, request: Request, json_request: dict):
        if (
            is_experimental_download_api(request)
            and json_request["request_type"] == "account"
            and "award_financial" in json_request["download_types"]
        ):
            # goes to spark for File C account download
            self.process_account_download_in_spark(download_job=download_job)
        elif settings.IS_LOCAL and settings.RUN_LOCAL_DOWNLOAD_IN_PROCESS:
            # Eagerly execute the download in this running process
            download_generation.generate_download(download_job)
        else:
            # Send a SQS message that will be processed by another server which will eventually run
            # download_generation.generate_download(download_source) (see download_sqs_worker.py)
            write_to_log(
                message=f"Passing download_job {download_job.download_job_id} to SQS", download_job=download_job
            )
            queue = get_sqs_queue(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody=str(download_job.download_job_id))

    def process_account_download_in_spark(self, download_job: DownloadJob):
        """
        Process File C downloads through spark instead of sqs for better performance
        """
        if settings.IS_LOCAL:
            spark_jobs = SparkJobs(LocalStrategy())
            spark_jobs.start(
                job_name="api_download-accounts",
                command_name="generate_spark_download",
                command_options=[f"--download-job-id={download_job.download_job_id}", "--skip-local-cleanup"],
            )
        else:
            spark_jobs = SparkJobs(DatabricksStrategy())
            spark_jobs.start(
                job_name="api_download-accounts",
                command_name="generate_spark_download",
                command_options=[f"--download-job-id={download_job.download_job_id}"],
            )

    def build_download_response(self, download_job: DownloadJob) -> Response:
        """
        Generate download response which encompasses various elements to provide accurate status for state of a
        download job
        """
        # Compile url to file
        file_path = get_file_path(download_job.file_name)

        # Generate the status endpoint for the file
        status_url = self._get_status_url(download_job.file_name)

        response = {
            "status_url": status_url,
            "file_name": download_job.file_name,
            "file_url": file_path,
            "download_request": json.loads(download_job.json_request),
        }

        return Response(response)

    def _get_status_url(self, file_name: str) -> str:
        if settings.IS_LOCAL:
            protocol = "http"
            host = "localhost:8000"
        else:
            protocol = "https"
            host = f"api.{settings.SERVER_BASE_URL}"
            if settings.DOWNLOAD_ENV != "production":
                host = f"{settings.DOWNLOAD_ENV}-{host}"

        return f"{protocol}://{host}/api/v2/download/status?file_name={file_name}"

    @staticmethod
    def _get_cached_download(
        ordered_json_request: str, download_types: Optional[List[str]] = None
    ) -> Optional[DownloadJob]:
        # External data types that directly affect download results
        if download_types and "elasticsearch_awards" in download_types:
            external_data_type_name_list = ["es_awards"]
        elif download_types and "elasticsearch_transactions" in download_types:
            external_data_type_name_list = ["es_transactions"]
        else:
            external_data_type_name_list = ["fpds", "fabs", "es_transactions", "es_awards"]

        external_data_type_id_list = [
            id for name, id in EXTERNAL_DATA_TYPE_DICT.items() if name in external_data_type_name_list
        ]

        # Clear the download "cache" based on most recent date in the list of relevant ExternalDataLoadDate objects
        updated_date_timestamp = ExternalDataLoadDate.objects.filter(
            external_data_type_id__in=external_data_type_id_list
        ).aggregate(Max("last_load_date"))["last_load_date__max"]

        # Conditional put in place for local development where the external dates may not be defined
        cached_download = None
        if updated_date_timestamp:
            recent_submission_window_date = DABSSubmissionWindowSchedule.objects.filter(
                submission_reveal_date__lt=datetime.max.replace(tzinfo=timezone.utc)
            ).aggregate(Max("submission_reveal_date"))["submission_reveal_date__max"]
            cached_download = (
                DownloadJob.objects.filter(
                    json_request=ordered_json_request,
                    update_date__gte=max(updated_date_timestamp, recent_submission_window_date),
                )
                .order_by("-update_date")
                .exclude(job_status_id=JOB_STATUS_DICT["failed"])
                .first()
            )
        return cached_download


def get_file_path(file_name: str) -> str:
    if settings.IS_LOCAL:
        file_path = settings.CSV_LOCAL_PATH + file_name
    else:
        s3_handler = S3Handler(
            bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, redirect_dir=settings.BULK_DOWNLOAD_S3_REDIRECT_DIR
        )
        file_path = s3_handler.get_simple_url(file_name=file_name)

    return file_path
