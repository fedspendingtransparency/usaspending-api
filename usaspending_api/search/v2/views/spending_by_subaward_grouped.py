import copy
import logging
from dataclasses import dataclass
from decimal import Decimal
from sys import maxsize
from typing import Any

from django.conf import settings
from elasticsearch_dsl import A
from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import SubawardSearch
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.query_types import SubawardSearchTimePeriod

logger = logging.getLogger(__name__)


@dataclass
class SubawardGroupedModel:
    award_id: int
    subaward_count: int
    award_generated_internal_id: str
    subaward_obligation: int


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

        query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters=filters, options=time_period_obj)
        results = sorted(
            self.build_elasticsearch_search_with_aggregation(filter_query),
            key=lambda x: x[self.pagination["sort_key"]],
            reverse=True if self.pagination["sort_order"] == "desc" else False,
        )

        return Response(self.construct_es_response(results))

    def validate_request_data(self, request_data: dict[str, Any]) -> dict[str, Any]:
        tiny_shield = TinyShield(self.models)
        return tiny_shield.block(request_data)

    def construct_es_response(self, results: list[SubawardGroupedModel]) -> dict[str, Any]:
        return {
            "limit": self.pagination["limit"],
            "results": results,
            "page_metadata": {
                "page": self.pagination["page"],
                "hasNext": True if len(results) > self.pagination["limit"] else False,
            },
            "messages": get_generic_filters_message(
                self.original_filters.keys(), [elem["name"] for elem in self.models]
            ),
        }

    def build_elasticsearch_search_with_aggregation(self, filter_query: ES_Q) -> dict[str, Any]:
        es_field_mapper = {
            "award_id": "_key",
            "award_generated_internal_id": "_key",
            "subaward_count": "_count",
            "subaward_obligation": "subaward_obligation",
        }

        # We're creating a custom key that concatenates the award_piid_fain (award_id) and unique_award_key
        #   to allow for sorting without creating nested buckets which we can't use for sorting.
        # The order of concatenation changes based on what field we're sorting on.
        match self.pagination["sort_key"]:
            case "award_generated_internal_id":
                agg_script = "doc['unique_award_key'].value + '|' + doc['award_piid_fain'].value"
            case _:
                agg_script = "doc['award_piid_fain'].value + '|' + doc['unique_award_key'].value"

        terms_aggregation = A(
            "terms",
            size=self.pagination["limit"],
            script={"source": agg_script, "lang": "painless"},
        )
        terms_aggregation.metric("subaward_obligation", "sum", field="subaward_amount").bucket(
            "sorted_subawards",
            "bucket_sort",
            sort={
                es_field_mapper[self.pagination["sort_key"]]: {"order": self.pagination["sort_order"]},
            },
            **{"from": (self.pagination["page"] - 1) * self.pagination["limit"], "size": self.pagination["limit"] + 1},
        )

        search_sum = SubawardSearch().filter(filter_query).extra(size=0)
        search_sum.aggs.bucket("subawards", terms_aggregation)

        response = search_sum.handle_execute()

        if response is None:
            raise Exception("Breaking generator, unable to reach cluster")

        results = []
        for result in response["aggregations"]["subawards"]["buckets"]:
            match self.pagination["sort_key"]:
                case "award_generated_internal_id":
                    award_generated_internal_id, award_id = result["key"].split("|")
                case _:
                    award_id, award_generated_internal_id = result["key"].split("|")

            subaward_obligation = Decimal(result["subaward_obligation"]["value"]).quantize(Decimal(".01"))

            item = SubawardGroupedModel(award_id, result["doc_count"], award_generated_internal_id, subaward_obligation)
            results.append(item)
        return [result.__dict__ for result in results]
