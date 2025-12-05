import copy
import logging
from decimal import Decimal
from sys import maxsize
from typing import Any

from django.conf import settings
from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
    under_development_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.query_types import SubawardSearchTimePeriod

logger = logging.getLogger(__name__)


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingBySubawardGroupedVisualizationViewSet(APIView):
    """
    This route takes award filters and returns the filtered awards ids, number of subawards for each award, \
    total amount of subaward obligation within each award and each award's generated internal id
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_subaward_grouped.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.original_filters: dict[str, Any] = {}
        self.pagination: dict[str, Any] = {}
        self.models = [
            {"name": "limits", "key": "limit", "type": "integer", "default": 10},
            {"name": "ordered", "key": "order", "type": "text", "text_type": "search", "default": "desc"},
            {
                "name": "object_class",
                "key": "filters|object class",
                "type": "array",
                "array_type": "integer",
                "text_type": "search",
                "array_max": maxsize,
            },
            {
                "name": "sorted",
                "key": "sort",
                "type": "enum",
                "enum_values": ["award_id", "subaward_count", "award_generated_internal_id", "subaward_obligation"],
                "text_type": "search",
                "default": "award_id",
            },
        ]

        # Accepts the same filters as spending_by_award
        self.models.extend(copy.deepcopy(AWARD_FILTER_NO_RECIPIENT_ID))
        self.models.extend(copy.deepcopy([model for model in PAGINATION if model["name"] != "sort"]))

    @cache_response()
    def post(self, request: Request) -> Response:
        """Return all subawards matching given awards"""
        self.original_filters = request.data.get("filters")
        json_request = self.validate_request_data(request.data)
        filters = json_request.get("filters", {})
        self.pagination = {
            "limit": json_request["limit"],
            "lower_bound": (json_request["page"] - 1) * json_request["limit"],
            "page": json_request["page"],
            "sort_key": json_request.get("sort"),
            "sort_order": json_request["order"],
            "upper_bound": json_request["page"] * json_request["limit"] + 1,
        }

        time_period_obj = SubawardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )

        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters=filters, options=time_period_obj)
        results = self.build_elasticsearch_search(filter_query)

        return Response(self.construct_es_response(results))

    def validate_request_data(self, request_data: dict[str, Any]) -> dict[str, Any]:
        tiny_shield = TinyShield(self.models)
        return tiny_shield.block(request_data)

    def construct_es_response(self, results: list[dict]) -> dict[str, Any]:
        return {
            "limit": self.pagination["limit"],
            "results": results,
            "page_metadata": {
                "page": self.pagination["page"],
                "hasNext": True if len(results) > self.pagination["limit"] else False,
            },
            "messages": [
                under_development_message(),
                *get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in self.models]),
            ],
        }

    def build_elasticsearch_search(self, filter_query: ES_Q) -> list[dict[str, Any]]:
        lower_limit = (self.pagination["page"] - 1) * self.pagination["limit"]

        # <Field in API request> : <Field name in ElasticSearch document>
        api_request_field_to_es_field_mapper = {
            "award_generated_internal_id": "generated_unique_award_id",
            "award_id": "display_award_id",
            "subaward_count": "subaward_count",
            "subaward_obligation": "total_subaward_amount",
        }

        search = (
            AwardSearch()
            .filter(filter_query)
            .source(fields=list(api_request_field_to_es_field_mapper.values()))
            .sort(
                {
                    api_request_field_to_es_field_mapper[self.pagination["sort_key"]]: {
                        "order": "asc" if self.pagination["sort_order"] == "asc" else "desc"
                    }
                }
            )
            .extra(from_=lower_limit, size=self.pagination["limit"])
        )
        es_response = search.handle_execute()

        if es_response is None:
            raise Exception("Breaking generator, unable to reach cluster")

        results = [
            {
                "award_id": source["_source"]["display_award_id"],
                "subaward_count": source["_source"]["subaward_count"],
                "subaward_obligation": (
                    Decimal(source["_source"]["total_subaward_amount"]).quantize(Decimal(".01"))
                    if source["_source"]["total_subaward_amount"]
                    else 0.0
                ),
                "award_generated_internal_id": source["_source"]["generated_unique_award_id"],
            }
            for source in es_response["hits"]["hits"]
        ]

        return results
