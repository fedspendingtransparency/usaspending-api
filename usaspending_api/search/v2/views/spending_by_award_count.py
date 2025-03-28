import copy
import logging
from collections import defaultdict

from sys import maxsize
from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView
from elasticsearch_dsl import Q

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.v2.views.enums import SpendingLevel

logger = logging.getLogger(__name__)


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardCountVisualizationViewSet(APIView):
    """This route takes award filters, and returns the number of awards in each award type.

    (Contracts, Loans, Grants, etc.)
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_award_count.md"

    @cache_response()
    def post(self, request):
        program_activities_rule = {
            "name": "program_activities",
            "type": "array",
            "key": "filters|program_activities",
            "array_type": "object",
            "object_keys_min": 1,
            "object_keys": {
                "name": {"type": "text", "text_type": "search"},
                "code": {
                    "type": "integer",
                },
            },
        }
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean"},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": [SpendingLevel.AWARD.value, SpendingLevel.SUBAWARD.value],
                "optional": True,
                "default": SpendingLevel.AWARD.value,
            },
            {
                "name": "object_class",
                "key": "filter|object_class",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
            {
                "name": "program_activity",
                "key": "filter|program_activity",
                "type": "array",
                "array_type": "integer",
                "array_max": maxsize,
            },
            program_activities_rule,
        ]
        models.extend(copy.deepcopy(AWARD_FILTER_NO_RECIPIENT_ID))
        models.extend(copy.deepcopy(PAGINATION))
        self.original_filters = request.data.get("filters")
        tiny_shield = TinyShield(models)

        json_request = tiny_shield.block(request.data)
        if "filters" in json_request and "program_activities" in json_request["filters"]:
            tiny_shield.enforce_object_keys_min(json_request, program_activities_rule)
        subawards = (
            json_request["subawards"]
            if "subawards" in json_request
            else True if json_request["spending_level"] == SpendingLevel.SUBAWARD.value else False
        )
        filters = json_request.get("filters", None)
        if filters is None:
            raise InvalidParameterException("Missing required request parameters: 'filters'")

        if "award_type_codes" in filters and "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            empty_results = {"contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}
            if subawards:
                empty_results = {"subcontracts": 0, "subgrants": 0}
            results = empty_results
        elif subawards:
            results = self.query_elasticsearch_for_subawards(filters)
        else:
            results = self.query_elasticsearch_for_prime_awards(filters)

        raw_response = {
            "results": results,
            "spending_level": "subawards" if subawards else json_request["spending_level"],
            "messages": get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in models]),
        }

        return Response(raw_response)

    def query_elasticsearch_for_prime_awards(self, filters) -> list:
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters)
        s = AwardSearch().filter(filter_query)

        s.aggs.bucket(
            "types",
            "filters",
            filters={category: Q("terms", type=types) for category, types in all_award_types_mappings.items()},
        )
        results = s.handle_execute()

        contracts = results.aggregations.types.buckets.contracts.doc_count
        idvs = results.aggregations.types.buckets.idvs.doc_count
        grants = results.aggregations.types.buckets.grants.doc_count
        direct_payments = results.aggregations.types.buckets.direct_payments.doc_count
        loans = results.aggregations.types.buckets.loans.doc_count
        other = results.aggregations.types.buckets.other_financial_assistance.doc_count

        response = {
            "contracts": contracts,
            "direct_payments": direct_payments,
            "grants": grants,
            "idvs": idvs,
            "loans": loans,
            "other": other,
        }
        return response

    def query_elasticsearch_for_subawards(self, filters) -> list:
        query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters)
        s = SubawardSearch().filter(filter_query).filter("exists", field="award_id").extra(size=0)

        s.aggs.bucket(
            "types",
            "terms",
            field="prime_award_group",
        )
        results = s.handle_execute().aggregations.types.buckets

        # transform results into a more readable dictionary
        raw_results = defaultdict(lambda: 0)
        for result in results:
            raw_results[result["key"]] = result["doc_count"]

        response = {
            "subgrants": raw_results["grant"],
            "subcontracts": raw_results["procurement"],
        }
        return response
