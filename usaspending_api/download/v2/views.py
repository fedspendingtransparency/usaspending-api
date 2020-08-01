import logging

from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet, DownloadRequestType

logger = logging.getLogger(__name__)


class RowLimitedAwardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of award data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/awards.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_awards", "sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.AWARD)


class RowLimitedIDVDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of IDV data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/idv.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.IDV)


class RowLimitedContractDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of Contract data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/contract.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.CONTRACT)


class RowLimitedAssistanceDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of Assistance data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/assistance.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.ASSISTANCE)


class RowLimitedTransactionDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of transaction data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/transactions.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_transactions", "sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.AWARD)


class AccountDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of account data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/accounts.md"

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, DownloadRequestType.ACCOUNT)


class DisasterDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of disaster data
    Specifically, only COVID-19 at the moment
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/disaster.md"

    def post(self, request):
        """Return url to pre-generated zip file"""

        return BaseDownloadViewSet.post(self, request, DownloadRequestType.DISASTER)


class DisasterRecipientDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of recipient disaster data
    Specifically, only COVID-19 at the moment
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/disaster/recipients.md"

    def post(self, request):
        return BaseDownloadViewSet.post(self, request, DownloadRequestType.DISASTER_RECIPIENT)
