from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet


class DownloadStatusViewSet(BaseDownloadViewSet):
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

        return self.get_download_response(file_name=file_name, is_download_status=True)
