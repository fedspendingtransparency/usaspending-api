import copy

from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import download_transaction_count
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield


class DownloadTransactionCountViewSet(APIDocumentationView):
    """
    Returns the number of transactions that would be included in a download request for the given filter set.

    endpoint_doc: /download/download_count.md
    """

    @cache_response()
    def post(self, request):
        """Returns boolean of whether a download request is greater than the max limit. """
        models = [{"name": "subawards", "key": "subawards", "type": "boolean", "default": False}]
        models.extend(copy.deepcopy(AWARD_FILTER))
        json_request = TinyShield(models).block(request.data)

        # If no filters in request return empty object to return all transactions
        filters = json_request.get("filters", {})

        is_over_limit = False

        if json_request["subawards"]:
            total_count = subaward_filter(filters).count()
        else:
            queryset, model = download_transaction_count(filters)
            if model in ["UniversalTransactionView"]:
                total_count = queryset.count()
            else:
                # "summary" materialized views are pre-aggregated and contain a counts col
                total_count = queryset.aggregate(total_count=Sum("counts"))["total_count"]

        if total_count and total_count > settings.MAX_DOWNLOAD_LIMIT:
            is_over_limit = True

        result = {"transaction_rows_gt_limit": is_over_limit}

        return Response(result)
