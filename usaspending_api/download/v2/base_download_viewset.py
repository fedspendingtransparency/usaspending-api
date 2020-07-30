import json

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from django.conf import settings
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.helpers.dict_helpers import order_nested_object
from usaspending_api.common.sqs.sqs_handler import get_sqs_queue
from usaspending_api.download.download_utils import create_unique_filename, log_new_download_job
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.download.helpers import write_to_download_log as write_to_log
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.request_validations import (
    validate_account_request,
    validate_assistance_request,
    validate_award_request,
    validate_contract_request,
    validate_disaster_recipient_request,
    validate_idv_request,
)


class DownloadRequestType(Enum):
    ACCOUNT = {"name": "account", "validate_func": validate_account_request}
    ASSISTANCE = {"name": "assistance", "validate_func": validate_assistance_request}
    AWARD = {"name": "award", "validate_func": validate_award_request}
    CONTRACT = {"name": "contract", "validate_func": validate_contract_request}
    DISASTER = {"name": "disaster"}
    DISASTER_RECIPIENT = {"name": "disaster_recipient", "validate_func": validate_disaster_recipient_request}
    IDV = {"name": "idv", "validate_func": validate_idv_request}


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseDownloadViewSet(APIView):
    def post(
        self,
        request: Request,
        request_type: DownloadRequestType = DownloadRequestType.AWARD,
        origination: Optional[str] = None,
    ):
        # This download is pre-generated and does not have the "request_type" property since it did not
        # go through our normal download generation process (it is added below). A check for this
        # in the json_request is used to determine uniqueness from other Disaster downloads.
        if request_type == DownloadRequestType.DISASTER:
            filename = (
                DownloadJob.objects.filter(
                    file_name__startswith=settings.COVID19_DOWNLOAD_FILENAME_PREFIX, error_message__isnull=True
                )
                .exclude(json_request__contains="request_type")
                .order_by("-update_date")
                .values_list("file_name", flat=True)
                .first()
            )
            return self.get_download_response(file_name=filename)

        json_request = request_type.value["validate_func"](request.data)
        json_request["request_type"] = request_type.value["name"]
        ordered_json_request = json.dumps(order_nested_object(json_request))

        # Check if the same request has been called today
        # TODO!!! Use external_data_load_date to determine data freshness
        updated_date_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d")
        cached_download = (
            DownloadJob.objects.filter(json_request=ordered_json_request, update_date__gte=updated_date_timestamp)
            .exclude(job_status_id=JOB_STATUS_DICT["failed"])
            .values("download_job_id", "file_name")
            .first()
        )

        if cached_download and not settings.IS_LOCAL:
            # By returning the cached files, there should be no duplicates on a daily basis
            write_to_log(message=f"Generating file from cached download job ID: {cached_download['download_job_id']}")
            cached_filename = cached_download["file_name"]
            return self.get_download_response(file_name=cached_filename)

        final_output_zip_name = create_unique_filename(json_request, origination=origination)
        download_job = DownloadJob.objects.create(
            job_status_id=JOB_STATUS_DICT["ready"], file_name=final_output_zip_name, json_request=ordered_json_request
        )

        log_new_download_job(request, download_job)
        self.process_request(download_job)

        return self.get_download_response(file_name=final_output_zip_name)

    def process_request(self, download_job: DownloadJob):
        if settings.IS_LOCAL and settings.RUN_LOCAL_DOWNLOAD_IN_PROCESS:
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

    def get_download_response(self, file_name: str):
        """
        Generate download response which encompasses various elements to provide accurate status for state of a
        download job
        """
        download_job = get_download_job(file_name)

        # Compile url to file
        file_path = get_file_path(file_name)

        # Generate the status endpoint for the file
        status_url = self._get_status_url(file_name)

        response = {
            "status_url": status_url,
            "file_name": file_name,
            "file_url": file_path,
            "download_request": json.loads(download_job.json_request),
        }

        return Response(response)

    def _get_status_url(self, file_name: str) -> str:
        if settings.IS_LOCAL:
            protocol = "http"
            host = "localhost:8000"
        elif settings.DOWNLOAD_ENV == "production":
            protocol = "https"
            host = "api.usaspending.gov"
        else:
            protocol = "https"
            host = f"{settings.DOWNLOAD_ENV}-api.usaspending.gov"

        return f"{protocol}://{host}/api/v2/download/status?file_name={file_name}"


def get_file_path(file_name: str) -> str:
    if settings.IS_LOCAL:
        file_path = settings.CSV_LOCAL_PATH + file_name
    else:
        s3_handler = S3Handler(
            bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, redirect_dir=settings.BULK_DOWNLOAD_S3_REDIRECT_DIR
        )
        file_path = s3_handler.get_simple_url(file_name=file_name)

    return file_path


def get_download_job(file_name: str) -> DownloadJob:
    download_job = DownloadJob.objects.filter(file_name=file_name).first()
    if not download_job:
        raise NotFound(f"Download job with filename {file_name} does not exist.")
    return download_job
