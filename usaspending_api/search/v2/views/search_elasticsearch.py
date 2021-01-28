import copy
import logging

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import (
    InvalidParameterException,
    UnprocessableEntityException,
)
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata, get_generic_filters_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER, AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_count
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_sum_and_count
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_SOURCE_LOOKUP, TRANSACTIONS_LOOKUP

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction.md"

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

        record_num = (validated_payload["page"] - 1) * validated_payload["limit"]
        if record_num >= settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW:
            raise UnprocessableEntityException(
                "Page #{page} of size {limit} is over the maximum result limit ({es_limit}). Consider using custom data downloads to obtain large data sets.".format(
                    page=validated_payload["page"],
                    limit=validated_payload["limit"],
                    es_limit=settings.ES_TRANSACTIONS_MAX_RESULT_WINDOW,
                )
            )

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
        sorts = {TRANSACTIONS_LOOKUP[validated_payload["sort"]]: validated_payload["order"]}
        lower_limit = (validated_payload["page"] - 1) * validated_payload["limit"]
        upper_limit = (validated_payload["page"]) * validated_payload["limit"] + 1
        validated_payload["filters"]["keyword_search"] = [
            es_minimal_sanitize(x) for x in validated_payload["filters"]["keywords"]
        ]
        validated_payload["filters"].pop("keywords")
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(validated_payload["filters"])
        search = TransactionSearch().filter(filter_query).sort(sorts)[lower_limit:upper_limit]
        response = search.handle_execute()
        return Response(self.build_elasticsearch_result(validated_payload, response))

    def build_elasticsearch_result(self, request, response) -> dict:
        results = []
        for res in response:
            hit = res.to_dict()
            # Parsing API response values from ES query result JSON
            # We parse the `hit` (result from elasticsearch) to get the award type, use the type to determine
            # which lookup dict to use, and then use that lookup to retrieve the correct value requested from `fields`
            row = {}
            for field in request["fields"]:
                row[field] = hit.get(TRANSACTIONS_SOURCE_LOOKUP[field])
            row["generated_internal_id"] = hit["generated_unique_award_id"]
            row["internal_id"] = hit["award_id"]

            results.append(row)

        metadata = get_simple_pagination_metadata(len(response), request["limit"], request["page"])

        return {
            "limit": request["limit"],
            "results": results[: request["limit"]],
            "page_metadata": metadata,
            "messages": [
                get_generic_filters_message(
                    request["filters"].keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
                )
            ],
        }


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
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


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_transaction_count.md"

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
        return Response({"results": results})
