import copy
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from django.conf import settings
from django.db.models import QuerySet, Sum
from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import ElasticsearchConnectionException, NotImplementedException
from usaspending_api.common.helpers.generic_helper import (
    get_simple_pagination_metadata,
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.search.v2.elasticsearch_helper import (
    get_number_of_unique_terms_for_transactions,
    get_number_of_unique_terms_for_awards,
    get_scaled_sum_aggregations,
)
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.query_types import TransactionSearchTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod

logger = logging.getLogger(__name__)


@dataclass
class Category:
    name: str
    agg_key: str


class AwardType(Enum):
    TRANSACTIONS = "transactions"
    AWARDS = "awards"
    SUBAWARDS = "subawards"


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class AbstractSpendingByCategoryViewSet(APIView, metaclass=ABCMeta):
    """
    Abstract class inherited by the different spending by category endpoints.
    """

    category: Category
    filters: dict
    obligation_column: str
    pagination: Pagination
    subawards: bool
    high_cardinality_categories: List[str] = ["recipient", "recipient_duns"]

    @cache_response()
    def post(self, request: Request) -> Response:
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False, "optional": True},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": ["awards", "transactions", "subawards"],
                "optional": True,
                "default": "transactions",
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        original_filters = request.data.get("filters")
        validated_payload = TinyShield(models).block(request.data)

        # Because Subawards and Transactions are associated with DEFCs via their Prime Award, which
        # contains a list of all DEFCs from its associated File C records, we need to limit results
        # to records with a sub_action_date and action_date, respectively, earlier than the enactment date
        # of the public law for each DEFC. This provides a more accurate breakdown of funds by DEFC.
        # This can be accomplished by simply adding an extra filter for all DEFCs because DEFC filtering
        # logic already takes this into account in query_with_filters.py. Because we are grouping by DEFC,
        # it is safe to filter down to only records that include one.
        # Check if it's the Spending by DEFC endpoint
        if self.category.name == "defc":
            # Get the list of DEFCs and add the filter if it doesn't already exist
            if "filters" not in validated_payload:
                validated_payload["filters"] = {}
            if "def_codes" not in validated_payload["filters"]:
                def_codes = list(DisasterEmergencyFundCode.objects.values_list("code", flat=True))
                validated_payload["filters"]["def_codes"] = def_codes

        raw_response = self.perform_search(validated_payload, original_filters)

        return Response(raw_response)

    def perform_search(self, validated_payload: dict, original_filters: dict) -> dict:

        self.filters = validated_payload.get("filters", {})
        self.subawards = validated_payload.get("subawards", False)  # Default to False if 'subawards' is not provided
        self.pagination = self._get_pagination(validated_payload)

        # Check for subawards conditions
        spending_level = validated_payload.get("spending_level")
        if self.subawards or AwardType(spending_level) == AwardType.SUBAWARDS:
            base_queryset = subaward_filter(self.filters)
            self.obligation_column = "subaward_amount"
            results = self.query_django_for_subawards(base_queryset)
        elif AwardType(spending_level) == AwardType.TRANSACTIONS:
            results = self.query_elasticsearch_for_transactions(validated_payload)
        else:
            results = self.query_elasticsearch_for_awards(validated_payload)

        page_metadata = get_simple_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        message_list = self._get_messages(original_filters)
        message_list.append(
            "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
            "See documentation for more information. "
        )

        response = {
            "category": self.category.name,
            "spending_level": validated_payload["spending_level"],
            "limit": self.pagination.limit,
            "page_metadata": page_metadata,
            "results": results[: self.pagination.limit],
            "messages": message_list,
        }

        return response

    def query_elasticsearch_for_transactions(self, validated_payload: dict) -> list:
        filter_options = {}
        time_period_obj = TransactionSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=_QueryType.TRANSACTIONS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters, **filter_options)
        results = self.query_elasticsearch(filter_query, validated_payload["spending_level"])

        return results

    def query_elasticsearch_for_awards(self, validated_payload: dict) -> list:
        options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=_QueryType.AWARDS
        )
        options["time_period_obj"] = new_awards_only_decorator
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters, **options)
        results = self.query_elasticsearch(filter_query, validated_payload["spending_level"])

        return results

    def _raise_not_implemented(self):
        msg = "Category '{}' is not implemented"
        if self.subawards:
            msg += " when `subawards` is True"
        raise NotImplementedException(msg.format(self.category.name))

    @staticmethod
    def _get_messages(original_filters) -> List:
        if original_filters:
            return get_generic_filters_message(original_filters.keys(), [elem["name"] for elem in AWARD_FILTER])
        else:
            return get_generic_filters_message(set(), [elem["name"] for elem in AWARD_FILTER])

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

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, spending_level: str
    ) -> Optional[TransactionSearch]:
        """
        Using the provided ES_Q object creates a TransactionSearch object with the necessary applied aggregations.
        """
        # Create the filtered Search Object
        search = None
        if AwardType(spending_level) == AwardType.TRANSACTIONS:
            search = TransactionSearch().filter(filter_query)
        elif AwardType(spending_level) == AwardType.AWARDS:
            search = AwardSearch().filter(filter_query)

        sum_aggregations = get_scaled_sum_aggregations("generated_pragmatic_obligation", self.pagination)

        # Need to handle high cardinality categories differently; this assumes that the Search object references
        # an Elasticsearch cluster that has a "routing" equal to "self.category.agg_key"
        if self.category.name in self.high_cardinality_categories:
            # 10k is the maximum number of allowed buckets
            size = self.pagination.upper_limit
            shard_size = size
            sum_bucket_sort = sum_aggregations["sum_bucket_truncate"]
            group_by_agg_key_values = {"order": {"sum_field": "desc"}}
        else:
            # Get count of unique buckets; terminate early if there are no buckets matching criteria
            bucket_count = None
            if spending_level == "transactions":
                bucket_count = get_number_of_unique_terms_for_transactions(
                    filter_query, f"{self.category.agg_key}.hash"
                )
            elif spending_level == "awards":
                bucket_count = get_number_of_unique_terms_for_awards(filter_query, f"{self.category.agg_key}.hash")

            if bucket_count == 0:
                return None
            else:
                # Add 100 to make sure that we consider enough records in each shard for accurate results;
                # Only needed for non high-cardinality fields since those are being routed
                size = bucket_count
                shard_size = bucket_count + 100
                sum_bucket_sort = sum_aggregations["sum_bucket_sort"]
                group_by_agg_key_values = {}

        if shard_size > 10000:
            logger.warning(f"Max number of buckets reached for aggregation key: {self.category.agg_key}.")
            raise ElasticsearchConnectionException(
                "Current filters return too many unique items. Narrow filters to return results."
            )

        # Define all aggregations needed to build the response
        group_by_agg_key_values.update({"field": self.category.agg_key, "size": size, "shard_size": shard_size})
        group_by_agg_key = A("terms", **group_by_agg_key_values)

        sum_field = sum_aggregations["sum_field"]

        # Apply the aggregations to the TransactionSearch object
        search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric("sum_field", sum_field).pipeline(
            "sum_bucket_sort", sum_bucket_sort
        )

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def query_elasticsearch(self, filter_query: ES_Q, spending_level: str) -> list:
        search = self.build_elasticsearch_search_with_aggregations(filter_query, spending_level)
        if search is None:
            return []
        response = search.handle_execute()
        results = self.build_elasticsearch_result(response.aggs.to_dict())
        return results

    @abstractmethod
    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        """
        Parses the response from Search.execute() as a dictionary and builds the results for the endpoint response.
        """
        pass

    @abstractmethod
    def query_django_for_subawards(self, base_queryset: QuerySet) -> List[dict]:
        """
        Sub-awards are still implemented with Postgres and thus this function is called when a request is received
        to query a category for sub-awards.
        """
        pass
