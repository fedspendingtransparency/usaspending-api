import copy
import itertools
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import List

from django.conf import settings
from django.db.models import QuerySet, Sum
from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata, get_time_period_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_terms

logger = logging.getLogger(__name__)


@dataclass
class Category:
    name: str
    primary_field: str


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class BaseSpendingByCategoryViewSet(APIView, metaclass=ABCMeta):
    """
    Base class inherited by the different category endpoints.
    """

    category: Category
    elasticsearch: bool
    filters: dict
    obligation_column: int
    pagination: Pagination
    subawards: bool

    @cache_response()
    def post(self, request: Request) -> Response:
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False, "optional": True},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        validated_payload = TinyShield(models).block(request.data)
        validated_payload["elasticsearch"] = is_experimental_elasticsearch_api(request)

        return Response(self.perform_search(validated_payload))

    def perform_search(self, validated_payload: dict) -> dict:

        self.filters = validated_payload.get("filters", {})
        self.elasticsearch = validated_payload.get("elasticsearch")
        self.subawards = validated_payload["subawards"]
        self.pagination = self._get_pagination(validated_payload)

        if self.subawards:
            base_queryset = subaward_filter(self.filters)
            self.obligation_column = "amount"
            results = self.query_django(base_queryset)
        elif self.elasticsearch:
            logger.info(
                f"Using experimental Elasticsearch functionality for 'spending_by_category/{self.category.name}'"
            )
            filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters)
            results = self.query_elasticsearch(filter_query)
        else:
            base_queryset = spending_by_category_view_queryset(self.category, self.filters)
            self.obligation_column = "generated_pragmatic_obligation"
            results = self.query_django(base_queryset)

        page_metadata = get_simple_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        response = {
            "category": self.category.name,
            "limit": self.pagination.limit,
            "page_metadata": page_metadata,
            "results": results[: self.pagination.limit],
            "messages": [get_time_period_message()],
        }

        return response

    @staticmethod
    def _get_pagination(payload):
        return Pagination(
            page=payload["page"],
            limit=payload["limit"],
            lower_limit=(payload["page"] - 1) * payload["limit"],
            upper_limit=payload["page"] * payload["limit"] + 1,
        )

    def common_db_query(self, queryset: QuerySet, django_filters: dict, django_values: list) -> QuerySet:
        return (
            queryset.filter(**django_filters)
            .values(*django_values)
            .annotate(amount=Sum(self.obligation_column))
            .order_by("-amount")
        )

    def elasticsearch_results_generator(self, filter_query: ES_Q, size: int = 1000) -> list:
        """
        The number of buckets required across the different categories varies. To solve this a generator is created
        with the assumption that the "apply_elasticsearch_aggregations" implementation makes use of partitions.
        This will allow for any number of results to be retrieved regardless of the number of buckets.
        """
        bucket_count = get_number_of_unique_terms(filter_query, self.category.primary_field)
        num_partitions = (bucket_count // size) + 1

        for partition in range(num_partitions):
            search = self.build_elasticsearch_search_with_aggregations(filter_query, partition, num_partitions, size)
            response = search.handle_execute()
            if response is None:
                raise Exception("Breaking generator, unable to reach cluster")
            results = self.build_elasticsearch_result(response.aggs.to_dict())
            yield results

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        results = self.elasticsearch_results_generator(filter_query)
        flat_results = list(itertools.chain.from_iterable(results))
        return flat_results

    @abstractmethod
    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        """
        Using the provided ES_Q object creates a TransactionSearch object with the necessary applied aggregations.
        All aggregations are applied in a specific order to insure the correct nested aggregations.

        The top-most aggregation is expected to use partitions to make sure that all buckets are retrieved.
        """
        pass

    @abstractmethod
    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        """
        Parses the response from Search.execute() as a dictionary and builds the results for the endpoint response.
        """
        pass

    @abstractmethod
    def query_django(self, base_queryset: QuerySet) -> List[dict]:
        pass
