import copy
import logging

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import (
    api_transformations,
    API_TRANSFORM_FUNCTIONS,
)
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_count
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize

logger = logging.getLogger(__name__)


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionCountVisualizationViewSet(APIView):
    """
    This route takes transaction search fields, and returns the transaction counts of the searched term.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction_count.md"

    @cache_response()
    def post(self, request):
        models = []
        models.extend(copy.deepcopy(AWARD_FILTER))
        for m in models:
            if m["name"] == "keywords":
                m["optional"] = True
            elif m["name"] == "keyword":
                m["optional"] = True
        validated_payload = TinyShield(models).block(request.data)
        if "keywords" in validated_payload["filters"]:
            validated_payload["filters"]["keyword_search"] = [
                es_minimal_sanitize(x) for x in validated_payload["filters"]["keywords"]
            ]
            validated_payload["filters"].pop("keywords")
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(validated_payload["filters"])
        search = TransactionSearch().filter(filter_query)
        results = spending_by_transaction_count(search)
        return Response({"results": results})
