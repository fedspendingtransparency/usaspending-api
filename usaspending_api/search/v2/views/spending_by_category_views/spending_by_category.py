import copy
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Union

from django.conf import settings
from elasticsearch_dsl import A, Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch, TransactionSearch
from usaspending_api.common.exceptions import ElasticsearchConnectionException, NotImplementedException
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message, get_simple_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.v2.elasticsearch_helper import (
    get_number_of_unique_terms,
    get_number_of_unique_terms_for_awards,
    get_number_of_unique_terms_for_transactions,
    get_scaled_sum_aggregations,
)
from usaspending_api.search.v2.views.enums import SpendingLevel

logger = logging.getLogger(__name__)


@dataclass
class Category:
    name: str
    agg_key: str


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class AbstractSpendingByCategoryViewSet(APIView, metaclass=ABCMeta):
    """
    Abstract class inherited by the different spending by category endpoints.
    """

    category: Category
    filters: dict
    obligation_column: str
    pagination: Pagination
    high_cardinality_categories: List[str] = ["recipient", "recipient_duns"]
    spending_level: Optional[SpendingLevel]
    subaward_agg_key_mapper = {
        "pop_country_agg_key": "sub_pop_country_agg_key",
        "pop_congressional_cur_agg_key": "sub_pop_congressional_cur_agg_key",
        "pop_county_agg_key": "sub_pop_county_agg_key",
        "pop_state_agg_key": "sub_pop_state_agg_key",
        "recipient_location_congressional_cur_agg_key": "sub_recipient_location_congressional_cur_agg_key",
        "recipient_location_county_agg_key": "sub_recipient_location_county_agg_key",
        "recipient_agg_key": "sub_recipient_agg_key",
    }

    @cache_response()
    def post(self, request: Request) -> Response:
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False, "optional": True},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": [level.value for level in SpendingLevel],
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
        self.pagination = self._get_pagination(validated_payload)

        self.spending_level = SpendingLevel(
            "subawards" if validated_payload.get("subawards") else validated_payload.get("spending_level")
        )

        # Federal accounts are not implemented for Subawards
        if self.category.name == "federal_account" and self.spending_level == SpendingLevel.SUBAWARD:
            self._raise_not_implemented()

        if self.spending_level == SpendingLevel.SUBAWARD:
            # Swap the agg_key fields for the equivilant Subaward fields, if applicable
            self.category.agg_key = self.subaward_agg_key_mapper.get(self.category.agg_key, self.category.agg_key)

            query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
            filter_query = query_with_filters.generate_elasticsearch_query(self.filters)
            results = self.query_elasticsearch(filter_query)
        elif self.spending_level == SpendingLevel.TRANSACTION:
            query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
            filter_query = query_with_filters.generate_elasticsearch_query(self.filters)
            results = self.query_elasticsearch(filter_query)
        else:
            query_with_filters = QueryWithFilters(QueryType.AWARDS)
            filter_query = query_with_filters.generate_elasticsearch_query(self.filters)
            results = self.query_elasticsearch(filter_query)

        page_metadata = get_simple_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        message_list = self._get_messages(original_filters)
        message_list.append(
            "'subawards' will be deprecated in the future. Set ‘spending_level’ to ‘subawards’ instead. "
            "See documentation for more information. "
        )

        response = {
            "category": self.category.name,
            "spending_level": self.spending_level.value,
            "limit": self.pagination.limit,
            "page_metadata": page_metadata,
            "results": results[: self.pagination.limit],
            "messages": message_list,
        }

        return response

    def _raise_not_implemented(self):
        msg = "Category '{}' is not implemented"
        if self.spending_level == SpendingLevel.SUBAWARD:
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

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q
    ) -> Optional[Union[AwardSearch, SubawardSearch, TransactionSearch]]:
        """
        Using the provided ES_Q object creates a TransactionSearch object with the necessary applied aggregations.
        """
        sum_aggregations = (
            get_scaled_sum_aggregations("subaward_amount", self.pagination)
            if self.spending_level == SpendingLevel.SUBAWARD
            else get_scaled_sum_aggregations("generated_pragmatic_obligation", self.pagination)
        )

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
            if self.spending_level == SpendingLevel.AWARD:
                bucket_count = get_number_of_unique_terms_for_awards(filter_query, f"{self.category.agg_key}.hash")
            elif self.spending_level == SpendingLevel.SUBAWARD:
                bucket_count = get_number_of_unique_terms(SubawardSearch, filter_query, f"{self.category.agg_key}.hash")
            else:
                bucket_count = get_number_of_unique_terms_for_transactions(
                    filter_query, f"{self.category.agg_key}.hash"
                )
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

        # Create the filtered Search Object
        if self.spending_level == SpendingLevel.AWARD:
            sum_as_cents_agg_outlay = A("sum", field="total_outlays", script={"source": "_value * 100"})
            sum_as_dollars_agg_outlay = A(
                "bucket_script",
                buckets_path={"sum_as_cents_outlay": "sum_as_cents_outlay"},
                script="params.sum_as_cents_outlay / 100",
            )
            group_by_agg_key.metric("sum_as_cents_outlay", sum_as_cents_agg_outlay).pipeline(
                "sum_as_dollars_outlay", sum_as_dollars_agg_outlay
            )
            search = AwardSearch().filter(filter_query)
        else:
            search = (
                SubawardSearch().filter(filter_query)
                if self.spending_level == SpendingLevel.SUBAWARD else TransactionSearch().filter(filter_query)
            )

        # Apply the aggregations to the TransactionSearch object
        search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric("sum_field", sum_field).pipeline(
            "sum_bucket_sort", sum_bucket_sort
        )

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        search = self.build_elasticsearch_search_with_aggregations(filter_query)
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
