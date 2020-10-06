from abc import abstractmethod
from typing import List, Optional, Dict

from django.conf import settings
from django.utils.functional import cached_property
from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AccountSearch
from usaspending_api.common.exceptions import ForbiddenException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, _BasePaginationMixin
from usaspending_api.search.v2.elasticsearch_helper import (
    get_scaled_sum_aggregations,
    get_number_of_unique_terms_for_awards,
    get_number_of_unique_nested_terms_accounts,
)


class ElasticsearchAccountSpendingPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {
        "obligation": "sum_covid_obligation",
        "outlay": "sum_covid_outlay",
    }
    sort_column_mapping = {
        "award_count": "count_awards_by_dim.award_count",
        "description": "_key",  # _key will ultimately sort on description value
        "code": "_key",  # Façade sort behavior, really sorting on description
        "id": "_key",  # Façade sort behavior, really sorting on description
        **sum_column_mapping,
    }

    @cached_property
    def pagination(self):
        return self.run_models(list(self.sort_column_mapping), default_sort_column="id")


class ElasticsearchLoansPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {"obligation": "sum_covid_obligation", "outlay": "sum_covid_outlay"}
    sort_column_mapping = {
        "award_count": "_count",
        "description": "_key",  # _key will ultimately sort on description value
        "code": "_key",  # Façade sort behavior, really sorting on description
        "id": "_key",  # Façade sort behavior, really sorting on description
        **sum_column_mapping,
    }

    @cached_property
    def pagination(self):
        return self.run_models(list(self.sort_column_mapping), default_sort_column="id")


class ElasticsearchAccountDisasterBase(DisasterBase):

    query_fields: List[str]
    agg_key: str
    agg_group_name: str = "group_by_agg_key"  # name used for the tier-1 aggregation group
    child_agg_key: str = None  # will drive including of a sub-bucket-aggregation if overridden by subclasses
    top_hits_fields: List[str]  # list used for the top_hits aggregation
    child_fields: List[str]  # list used for the top_hits aggregation
    filter_query: ES_Q
    bucket_count: int

    pagination: Pagination  # Overwritten by a pagination mixin
    sort_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin
    sum_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin

    @cache_response()
    def post(self, request: Request) -> Response:
        # Need to update the value of "query" to have the fields to search on
        query = self.filters.pop("query", None)
        if query:
            self.filters["query"] = {"text": query, "fields": self.query_fields}

        # Ensure that only non-zero values are taken into consideration
        # TODO: Refactor to use new NonzeroFields filter in QueryWithFilters
        non_zero_queries = []
        for field in self.sum_column_mapping.values():
            non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
        self.filter_query.must.append(ES_Q("bool", should=non_zero_queries, minimum_should_match=1))

        self.bucket_count = get_number_of_unique_terms_for_awards(self.filter_query, f"{self.agg_key}.hash")
        messages = []
        if self.pagination.sort_key in ("id", "code"):
            messages.append(
                (
                    f"Notice! API Request to sort on '{self.pagination.sort_key}' field isn't fully implemented."
                    " Results were actually sorted using 'description' field."
                )
            )
        if self.bucket_count > 10000 and self.agg_key == settings.ES_ROUTING_FIELD:
            self.bucket_count = 10000
            messages.append(
                (
                    "Notice! API Request is capped at 10,000 results. Either download to view all results or"
                    " filter using the 'query' attribute."
                )
            )

        response = self.query_elasticsearch()
        response["page_metadata"] = get_pagination_metadata(
            self.bucket_count, self.pagination.limit, self.pagination.page
        )
        if messages:
            response["messages"] = messages

        return Response(response)

    @abstractmethod
    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        pass

    def perform_search(self, agg_key, search_fields):
        bucket_count = get_number_of_unique_nested_terms_accounts(self.filter_query, f"{agg_key}")
        size = bucket_count
        shard_size = bucket_count + 100

        search = AccountSearch().filter(self.filter_query)
        aggs = A("nested", path="financial_accounts_by_award")
        group_by_dim_agg = A("terms", field=agg_key, size=size, shard_size=shard_size)
        dim_metadata = A(
            "top_hits",
            size=1,
            sort=[{"financial_accounts_by_award.update_date": {"order": "desc"}}],
            _source={"includes": search_fields},
        )
        group_by_dim_agg.metric("dim_metadata", dim_metadata)
        #
        # pagination_agg = A(
        #     "bucket_sort",
        #     sort={self.sort_column_mapping[self.pagination.sort_key]: {"order": self.pagination.sort_order}},
        # )
        # group_by_dim_agg.metric("pagination_agg", pagination_agg)
        sum_covid_outlay = A(
            "sum",
            field="financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
            script={"source": "doc['financial_accounts_by_award.is_final_balances_for_fy'].value ? _value : 0"},
        )
        group_by_dim_agg.metric("sum_covid_outlay", sum_covid_outlay)
        sum_covid_obligation = A("sum", field="financial_accounts_by_award.transaction_obligated_amount")
        group_by_dim_agg.metric("sum_covid_obligation", sum_covid_obligation)
        value_count = A("cardinality", field="financial_account_distinct_award_key")
        count_awards_by_dim = A("reverse_nested", **{})
        count_awards_by_dim.metric("award_count", value_count)
        group_by_dim_agg.metric("count_awards_by_dim", count_awards_by_dim)
        aggs.metric("group_by_dim_agg", group_by_dim_agg)
        search.aggs.bucket("financial_accounts_agg", aggs)
        search.update_from_dict({"size": 0})
        return search

    def build_elasticsearch_search_with_aggregations(self) -> Optional[AccountSearch]:
        """
        Using the provided ES_Q object creates an AccountSearch object with the necessary applied aggregations.
        """
        # Create the initial search using filters
        return self.perform_search(self.agg_key, self.top_hits_fields)

    def build_totals(self, response: List[dict]) -> dict:
        obligations = 0
        outlays = 0
        award_count = 0
        for item in response:
            obligations += item["sum_covid_obligation"]["value"]
            outlays += item["sum_covid_outlay"]["value"]
            award_count += item["count_awards_by_dim"]["award_count"]["value"]

        return {"obligation": obligations, "outlay": outlays, "award_count": award_count}

    def query_elasticsearch(self) -> dict:
        search = self.build_elasticsearch_search_with_aggregations()
        if search is None:
            totals = self.build_totals(response=[])
            return {"totals": totals, "results": []}

        response = search.handle_execute()
        response = response.aggs.to_dict()
        buckets = response.get("financial_accounts_agg", {}).get("group_by_dim_agg", {}).get("buckets", [])
        totals = self.build_totals(buckets)
        results = self.build_elasticsearch_result(buckets)

        return {"results": results, "totals": totals}
