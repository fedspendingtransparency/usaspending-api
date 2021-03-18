from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet
from usaspending_api.download.v2.request_validations import AwardDownloadValidator


class YearLimitedDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md"

    def post(self, request):
        request.data["constraint_type"] = "year"
        return BaseDownloadViewSet.post(
            self, request, origination="bulk_download", validator_type=AwardDownloadValidator
        )
