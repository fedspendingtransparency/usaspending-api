import copy
import logging

from sys import maxsize
from django.conf import settings
from django.db.models import Count, Sum
from rest_framework.response import Response
from rest_framework.views import APIView
from elasticsearch_dsl import Search

from usaspending_api.awards.v2.filters.filter_helpers import add_date_range_comparison_types
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_award_count
from usaspending_api.awards.v2.lookups.lookups import all_awards_types_to_category
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_connectors.spending_by_award_count_asyncpg import fetch_all_category_counts
from usaspending_api.common.elasticsearch.client import es_client_query_count
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.common.helpers.orm_helpers import category_to_award_materialized_views
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield

logger = logging.getLogger("console")


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardCountVisualizationViewSet(APIView):
    """This route takes award filters, and returns the number of awards in each award type.

    (Contracts, Loans, Grants, etc.)
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_award_count.md"

    @cache_response()
    def post(self, request):
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
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
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)
        subawards = json_request["subawards"]
        filters = add_date_range_comparison_types(
            json_request.get("filters", None), subawards, gte_date_type="action_date", lte_date_type="date_signed"
        )
        elasticsearch = is_experimental_elasticsearch_api(request)

        if elasticsearch and not subawards:
            logger.info("Using experimental Elasticsearch functionality for 'spending_by_award'")
            results = self.query_elasticsearch(filters)
            return Response({"results": results, "messages": [get_time_period_message()]})

        if filters is None:
            raise InvalidParameterException("Missing required request parameters: 'filters'")

        empty_results = {"contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}
        if subawards:
            empty_results = {"subcontracts": 0, "subgrants": 0}

        if "award_type_codes" in filters and "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response({"results": empty_results})

        if subawards:
            results = self.handle_subawards(filters)
        else:
            results = self.handle_awards(filters, empty_results)

        return Response({"results": results, "messages": [get_time_period_message()]})

    @staticmethod
    def handle_awards(filters: dict, results_object: dict) -> dict:
        """Turn the filters into the result dictionary when dealing with Awards

        For performance reasons, there are two execution paths. One is to use a "summary"
        award materialized view which contains a subset of fields and has some aggregation
        to speed up the large general queries.

        For more specific queries, use concurrent SQL queries to obtain the counts
        """
        queryset, model = spending_by_award_count(filters)  # Will return None, None if it cannot use a summary matview

        if not model:  # DON'T use `queryset` in the conditional! Wasteful DB query
            return fetch_all_category_counts(filters, category_to_award_materialized_views())

        queryset = queryset.values("type").annotate(category_count=Sum("counts"))

        for award in queryset:
            if award["type"] is None or award["type"] not in all_awards_types_to_category:
                result_key = "other"
            else:
                result_key = all_awards_types_to_category[award["type"]]
                if result_key == "other_financial_assistance":
                    result_key = "other"

            results_object[result_key] += award["category_count"]
        return results_object

    @staticmethod
    def handle_subawards(filters: dict) -> dict:
        """Turn the filters into the result dictionary when dealing with Sub-Awards

        Note: Due to how the Django ORM joins to the awards table as an
        INNER JOIN, it is necessary to explicitly enforce the aggregations
        to only count Sub-Awards that are linked to a Prime Award.

        Remove the filter and update if we can move away from this behavior.
        """
        queryset = (
            subaward_filter(filters)
            .filter(award_id__isnull=False)
            .values("award_type")
            .annotate(count=Count("subaward_id"))
        )

        results = {}
        results["subgrants"] = sum([sub["count"] for sub in queryset if sub["award_type"] == "grant"])
        results["subcontracts"] = sum([sub["count"] for sub in queryset if sub["award_type"] == "procurement"])

        return results

    def query_elasticsearch(self, filters) -> list:
        filter_query = QueryWithFilters.generate_elasticsearch_query(filters, query_type="awards")
        contracts = Search(index="{}-contracts".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(filter_query)
        idvs = Search(index="{}-idvs".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(filter_query)
        grants = Search(index="{}-grants".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(filter_query)
        directpayments = Search(index="{}-directpayments".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(
            filter_query
        )
        loans = Search(index="{}-loans".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(filter_query)
        other = Search(index="{}-other".format(settings.ES_AWARDS_QUERY_ALIAS_PREFIX)).filter(filter_query)
        response = {
            "contracts": es_client_query_count(search=contracts),
            "direct_payments": es_client_query_count(search=directpayments),
            "grants": es_client_query_count(search=grants),
            "idvs": es_client_query_count(search=idvs),
            "loans": es_client_query_count(search=loans),
            "other": es_client_query_count(search=other),
        }
        return response
