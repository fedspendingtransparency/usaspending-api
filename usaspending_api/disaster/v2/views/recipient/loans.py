import json
import re
from decimal import Decimal
from typing import List, Optional

from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import ElasticsearchConnectionException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, RecipientLoansMixin
from usaspending_api.search.v2.elasticsearch_helper import (
    get_scaled_sum_aggregations,
    get_number_of_unique_terms_for_awards,
)


class RecipientLoans(RecipientLoansMixin, DisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Loans by Recipient.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/loans.md"
    required_filters = ["def_codes", "query"]

    @cache_response()
    def post(self, request: Request) -> Response:
        # Handle "query" filter outside of QueryWithFilters
        query = self.filters.pop("query", None)

        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)

        if query:
            filter_query.must.append(
                ES_Q("query_string", query=query, default_operator="OR", fields=["recipient_name"])
            )

        # Ensure that only non-zero values are taken into consideration
        non_zero_queries = []
        for field in self.sum_column_mapping.values():
            non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
        filter_query.must.append(ES_Q("bool", should=non_zero_queries, minimum_should_match=1))

        results = self.query_elasticsearch(filter_query)

        # Get a count of recipients for pagination metadata;
        # Defaults to 10k if greater than since that is the cap for Elasticsearch aggregations
        total_results = get_number_of_unique_terms_for_awards(filter_query, "recipient_agg_key.hash")

        return Response(
            {
                "results": results[: self.pagination.limit],
                "page_metadata": get_pagination_metadata(total_results, self.pagination.limit, self.pagination.page),
            }
        )

    def build_elasticsearch_search_with_aggregations(self, filter_query: ES_Q) -> Optional[AwardSearch]:
        """
        Using the provided ES_Q object creates an AwardSearch object with the necessary applied aggregations.
        """
        # Create the initial search using filters
        search = AwardSearch().filter(filter_query)

        if self.pagination.upper_limit > 10000:
            raise ElasticsearchConnectionException(
                "Current filters return too many unique items. Narrow filters to return results."
            )

        # Define all aggregations needed to build the response
        group_by_agg_key = A(
            "terms",
            field="recipient_agg_key",
            size=self.pagination.upper_limit,
            shard_size=self.pagination.upper_limit,
            order={self.sort_column_mapping[self.pagination.sort_key]: self.pagination.sort_order},
        )
        pagination_aggregation = A(
            "bucket_sort",
            **{"from": (self.pagination.page - 1) * self.pagination.limit, "size": self.pagination.limit + 1},
        )
        sum_aggregations = {
            mapping: get_scaled_sum_aggregations(mapping, self.pagination)
            for mapping in self.sum_column_mapping.values()
        }

        # Define all aggregations needed to build the response
        search.aggs.bucket("group_by_agg_key", group_by_agg_key)
        for field, sum_aggregations in sum_aggregations.items():
            search.aggs["group_by_agg_key"].metric(field, sum_aggregations["sum_field"])
        search.aggs["group_by_agg_key"].pipeline("pagination_aggregation", pagination_aggregation)

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in info_buckets:
            info = json.loads(bucket.get("key"))

            # Build a list of hash IDs to handle multiple levels
            recipient_hash = info.get("hash")
            recipient_levels = sorted(list(re.sub("[{},]", "", info.get("levels", ""))))
            if recipient_hash and recipient_levels:
                recipient_hash_list = [f"{recipient_hash}-{level}" for level in recipient_levels]
            else:
                recipient_hash_list = None

            results.append(
                {
                    "id": recipient_hash_list,
                    "code": info["unique_id"] or "DUNS Number not provided",
                    "description": info["name"] or None,
                    "count": int(bucket.get("doc_count", 0)),
                    **{
                        column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                        for column in self.sum_column_mapping
                    },
                }
            )

        return results

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        search = self.build_elasticsearch_search_with_aggregations(filter_query)
        response = search.handle_execute()
        results = self.build_elasticsearch_result(response.aggs.to_dict())
        return results
