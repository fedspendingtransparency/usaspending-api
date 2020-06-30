import logging

from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet

logger = logging.getLogger("console")


class RowLimitedAwardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/awards.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_awards", "sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, "award")


class RowLimitedIDVDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of IDV data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/idv.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, "idv")


class RowLimitedContractDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of Contract data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/contract.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, "contract")


class RowLimitedAssistanceDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of Assistance data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/assistance.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, "assistance")


class RowLimitedTransactionDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for
    download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/transactions.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_transactions", "sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, "award")


class AccountDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zipfile of account data in CSV form for download.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/accounts.md"

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, "account")

 # dev-5507
class AccountDownloadPeriodViewSet():
    #TODO : endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/periods.md"

    def get(self, request):
        return