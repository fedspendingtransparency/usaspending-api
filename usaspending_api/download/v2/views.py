from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet


class RowLimitedAwardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

    endpoint_doc: /download/advanced_search_award_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['awards', 'sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class RowLimitedIDVDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of IDV data in CSV form for download.

    endpoint_doc: /download/idv_download.md
    """

    def post(self, request):
        return BaseDownloadViewSet.post(self, request, 'idv')


class RowLimitedTransactionDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for
    download.

    endpoint_doc: /download/advanced_search_transaction_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['transactions', 'sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class RowLimitedSubawardDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of subaward data in CSV form for download.

    endpoint_doc: /download/advanced_search_subaward_download.md
    """

    def post(self, request):
        request.data['award_levels'] = ['sub_awards']
        request.data['constraint_type'] = 'row_count'
        return BaseDownloadViewSet.post(self, request, 'award')


class AccountDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to begin generating a zipfile of account data in CSV form for download.

    endpoint_doc: /download/custom_account_data_download.md
    """

    def post(self, request):
        """Push a message to SQS with the validated request JSON"""

        return BaseDownloadViewSet.post(self, request, 'account')
