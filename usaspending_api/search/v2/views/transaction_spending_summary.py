import logging

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_sum_and_count

logger = logging.getLogger(__name__)


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class TransactionSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of transactions and summation of federal action obligations.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/transaction_spending_summary.md"

    @cache_response()
    def post(self, request):
        """
        Returns a summary of transactions which match the award search filter
            Desired values:
                total number of transactions `award_count`
                The federal_action_obligation sum of all those transactions `award_spending`

        *Note* Only deals with prime awards, future plans to include sub-awards.
        """

        models = [
            {
                "name": "keywords",
                "key": "filters|keywords",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
                "optional": False,
                "text_min": 3,
            }
        ]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_sum_and_count(validated_payload)
        return Response({"results": results})
