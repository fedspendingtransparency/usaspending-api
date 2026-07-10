import logging

from django.db import connections
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.v2.base_download_viewset import get_file_path
from usaspending_api.routers.replicas import ReadReplicaRouter

logger = logging.getLogger(__name__)
FAILED_DOWNLOAD_MESSAGE = (
    "An error occurred while generating the download. Please try again"
)


class DownloadStatusViewSet(APIView):
    """
    This route gets the current status of a download job that that has been requested with the
    `v2/download/awards/` or `v2/download/transaction/` endpoint that same day. Accessed by both
    `v2/download/status/?file_name=""` and `v2/bulk_download/status/?file_name=""`.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/status.md"

    def get(self, request: Request) -> Response:
        """Obtain status for the download job matching the file name provided"""
        get_request = request.query_params
        file_name = get_request.get("file_name")

        if not file_name:
            raise InvalidParameterException("Missing one or more required query parameters: file_name")

        return self.get_download_status_response(file_name=file_name)

    def get_download_status_response(self, file_name: str) -> Response:
        """
        Generate download status response which encompasses various elements to provide accurate
        status for state of a download job
        """
        download_job = self.get_download_job(file_name)

        # Compile url to file
        file_path = get_file_path(file_name)

        response = {
            "status": download_job.job_status.name,
            "message": self._get_user_message(download_job),
            "file_name": file_name,
            "file_url": file_path,
            # converting size from bytes to kilobytes if file_size isn't None
            "total_size": download_job.file_size / 1000 if download_job.file_size else None,
            "total_columns": download_job.number_of_columns,
            "total_rows": download_job.number_of_rows,
            "seconds_elapsed": download_job.seconds_elapsed(),
        }

        return Response(response)

    def _get_user_message(self, download_job: DownloadJob) -> str | None:
        if download_job.job_status.name != "failed":
            return None

        if download_job.error_message:
            logger.error(
                "Download job %s failed: %s",
                download_job.file_name,
                download_job.error_message,
            )
        return FAILED_DOWNLOAD_MESSAGE

    def get_download_job(self, file_name: str) -> DownloadJob:
        # If we have a read replicas connection defined, then use that connection for querying the download_job
        #    table, otherwise use the default connection
        read_replica = ReadReplicaRouter.read_replicas[0]

        db_connections = connections.__dict__["_settings"]

        if read_replica in db_connections.keys():
            download_job = DownloadJob.objects.using(read_replica).filter(file_name=file_name).first()
        else:
            download_job = DownloadJob.objects.filter(file_name=file_name).first()

        if not download_job:
            raise NotFound(f"Download job with filename {file_name} does not exist.")
        return download_job
