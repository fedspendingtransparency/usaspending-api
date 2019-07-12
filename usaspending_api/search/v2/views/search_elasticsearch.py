import copy
import logging

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import ElasticsearchConnectionException, InvalidParameterException
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import search_transactions
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_count
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_sum_and_count

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction.md
    """

    @cache_response()
    def post(self, request):

        models = [
            {
                "name": "fields",
                "key": "fields",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
                "optional": False,
            }
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m["name"] in ("keywords", "award_type_codes", "sort"):
                m["optional"] = False
        validated_payload = TinyShield(models).block(request.data)

        if validated_payload["sort"] not in validated_payload["fields"]:
            raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload["sort"]))

        if "filters" in validated_payload and "no intersection" in validated_payload["filters"]["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response(
                {
                    "limit": validated_payload["limit"],
                    "results": [],
                    "page_metadata": {
                        "page": validated_payload["page"],
                        "next": None,
                        "previous": None,
                        "hasNext": False,
                        "hasPrevious": False,
                    },
                }
            )

        lower_limit = (validated_payload["page"] - 1) * validated_payload["limit"]
        success, response, total = search_transactions(validated_payload, lower_limit, validated_payload["limit"] + 1)
        if not success:
            raise InvalidParameterException(response)

        metadata = get_simple_pagination_metadata(len(response), validated_payload["limit"], validated_payload["page"])

        results = []
        for transaction in response[: validated_payload["limit"]]:
            results.append(transaction)

        response = {"limit": validated_payload["limit"], "results": results, "page_metadata": metadata}
        return Response(response)


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class TransactionSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of transactions and summation of federal action obligations.
        endpoint_doc: /advanced_award_search/transaction_spending_summary.md
    """

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
        if not results:
            raise ElasticsearchConnectionException("Error generating the transaction sums and counts")
        return Response({"results": results})


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction_count.md

    """

    @cache_response()
    def post(self, request):

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
        results = spending_by_transaction_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException("Error during the aggregations")
        return Response({"results": results})
