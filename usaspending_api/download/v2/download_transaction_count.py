import copy

from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import download_transaction_count
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield


class DownloadTransactionCountViewSet(APIView):
    """
    Returns the number of transactions that would be included in a download request for the given filter set.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/download/count.md"

    @cache_response()
    def post(self, request):
        """Returns boolean of whether a download request is greater than the max limit. """
        models = [{"name": "subawards", "key": "subawards", "type": "boolean", "default": False}]
        models.extend(copy.deepcopy(AWARD_FILTER))
        self.original_filters = request.data.get("filters")
        json_request = TinyShield(models).block(request.data)

        # If no filters in request return empty object to return all transactions
        filters = json_request.get("filters", {})

        if json_request["subawards"]:
            total_count = subaward_filter(filters).count()
        else:
            queryset, model = download_transaction_count(filters)
            if model in ["UniversalTransactionView"]:
                total_count = queryset.count()
            else:
                # "summary" materialized views are pre-aggregated and contain a counts col
                total_count = queryset.aggregate(total_count=Sum("counts"))["total_count"]

        if total_count is None:
            total_count = 0

        result = {
            "calculated_transaction_count": total_count,
            "maximum_transaction_limit": settings.MAX_DOWNLOAD_LIMIT,
            "transaction_rows_gt_limit": total_count > settings.MAX_DOWNLOAD_LIMIT,
            "messages": [
                get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER])
            ],
        }

        return Response(result)
