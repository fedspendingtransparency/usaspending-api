import logging

from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet
from usaspending_api.download.v2.request_validations import (
    AccountDownloadValidator,
    AssistanceDownloadValidator,
    AwardDownloadValidator,
    ContractDownloadValidator,
    DisasterDownloadValidator,
    DisasterRecipientDownloadValidator,
    IdvDownloadValidator,
)

logger = logging.getLogger(__name__)


class RowLimitedAwardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of award data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/awards.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_awards", "elasticsearch_sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, validator_type=AwardDownloadValidator)


class RowLimitedIDVDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of IDV data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/idv.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, validator_type=IdvDownloadValidator)


class RowLimitedContractDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of Contract data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/contract.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, validator_type=ContractDownloadValidator)


class RowLimitedAssistanceDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of Assistance data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/assistance.md"

    def post(self, request):
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, validator_type=AssistanceDownloadValidator)


class RowLimitedTransactionDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of transaction data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/transactions.md"

    def post(self, request):
        request.data["award_levels"] = ["elasticsearch_transactions", "elasticsearch_sub_awards"]
        request.data["constraint_type"] = "row_count"
        return BaseDownloadViewSet.post(self, request, validator_type=AwardDownloadValidator)


class AccountDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of account data
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/accounts.md"

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, validator_type=AccountDownloadValidator)


class DisasterDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of disaster data
    Specifically, only COVID-19 at the moment
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/disaster.md"

    def post(self, request):
        """Return url to pre-generated zip file"""

        return BaseDownloadViewSet.post(self, request, validator_type=DisasterDownloadValidator)


class DisasterRecipientDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zip file of recipient disaster data
    Specifically, only COVID-19 at the moment
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/disaster/recipients.md"

    def post(self, request):
        return BaseDownloadViewSet.post(self, request, validator_type=DisasterRecipientDownloadValidator)
