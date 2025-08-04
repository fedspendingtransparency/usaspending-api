from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.base_download_viewset import get_download_job, get_file_path


class DownloadStatusViewSet(APIView):
    """
    This route gets the current status of a download job that that has been requested with the
    `v2/download/awards/` or `v2/download/transaction/` endpoint that same day. Accessed by both
    `v2/download/status/?file_name=""` and `v2/bulk_download/status/?file_name=""`.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/status.md"

    def get(self, request):
        """Obtain status for the download job matching the file name provided"""
        get_request = request.query_params
        file_name = get_request.get("file_name")

        if not file_name:
            raise InvalidParameterException("Missing one or more required query parameters: file_name")

        return self.get_download_status_response(file_name=file_name)

    def get_download_status_response(self, file_name: str):
        """
        Generate download status response which encompasses various elements to provide accurate
        status for state of a download job
        """
        download_job = get_download_job(file_name)

        # Compile url to file
        file_path = get_file_path(file_name)

        response = {
            "status": download_job.job_status.name,
            "message": download_job.error_message,
            "file_name": file_name,
            "file_url": file_path,
            # converting size from bytes to kilobytes if file_size isn't None
            "total_size": download_job.file_size / 1000 if download_job.file_size else None,
            "total_columns": download_job.number_of_columns,
            "total_rows": download_job.number_of_rows,
            "seconds_elapsed": download_job.seconds_elapsed(),
        }

        return Response(response)
