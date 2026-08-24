from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.spending_explorer.v2.filters.explorer import SPENDING_EXPLORER_LIMIT
from usaspending_api.spending_explorer.v2.filters.type_filter import type_filter


class SpendingExplorerViewSet(APIView):
    """
    This route sends a request to the backend to retrieve spending data information through various types and filters.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/spending.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        json_request = request.data
        _type = json_request.get("type")
        filters = json_request.get("filters", None)

        # Returned filtered queryset results
        results = type_filter(_type, filters, SPENDING_EXPLORER_LIMIT)

        return Response(results)
