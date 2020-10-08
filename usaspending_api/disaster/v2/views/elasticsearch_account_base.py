from abc import abstractmethod
from typing import List, Optional, Dict

from django.utils.functional import cached_property
from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.response import Response

from usaspending_api import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AccountSearch
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, _BasePaginationMixin
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_nested_terms_accounts


class ElasticsearchSpendingPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {
        "obligation": "financial_accounts_by_award.transaction_obligated_amount",
        "outlay": "financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
    }
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


class ElasticsearchLoansPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {
        "obligation": "total_covid_obligation",
        "outlay": "total_covid_outlay",
        "face_value_of_loan": "total_loan_value",
    }
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

    agg_group_name: str = "group_by_agg_key"  # name used for the tier-1 aggregation group
    agg_key: str
    bucket_count: int
    filter_query: ES_Q
    has_children: bool = False
    query_fields: List[str]
    sub_agg_group_name: str = "sub_group_by_sub_agg_key"  # name used for the tier-2 aggregation group
    sub_agg_key: str = None  # will drive including of a sub-bucket-aggregation if overridden by subclasses
    top_hits_fields: List[str]  # list used for the top_hits aggregation

    pagination: Pagination  # Overwritten by a pagination mixin
    sort_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin
    sum_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin

    @cache_response()
    def post(self, request):
        return self.perform_elasticsearch_search()

    def perform_elasticsearch_search(self) -> Response:
        # Need to update the value of "query" to have the fields to search on
        defc = self.filters.pop("def_codes")
        if self.filters.get("award_type_codes"):
            award_types = self.filters.pop("award_type_codes")
            self.filters.update({"award_type": award_types})
        self.filters.update({"nested_def_codes": defc})
        self.filters.update(
            {
                "nested_nonzero_fields": [
                    "financial_accounts_by_award.transaction_obligated_amount",
                    "financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
                ]
            }
        )
        self.filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(self.filters)
        self.bucket_count = get_number_of_unique_nested_terms_accounts(self.filter_query, f"{self.agg_key}")
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

    def build_elasticsearch_search_with_aggregations(self) -> Optional[AccountSearch]:
        """
        Using the provided ES_Q object creates an AccountSearch object with the necessary applied aggregations.
        """
        # Create the initial search using filters
        search = AccountSearch().filter(self.filter_query)

        # Create the aggregations
        aggs = A("nested", path="financial_accounts_by_award")
        group_by_dim_agg = A("terms", field=self.agg_key)
        dim_metadata = A(
            "top_hits",
            size=1,
            sort=[{"financial_accounts_by_award.update_date": {"order": "desc"}}],
            _source={"includes": self.top_hits_fields},
        )
        sum_covid_outlay = A(
            "sum",
            field="financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
            script={"source": "doc['financial_accounts_by_award.is_final_balances_for_fy'].value ? _value : 0"},
        )
        sum_covid_obligation = A("sum", field="financial_accounts_by_award.transaction_obligated_amount")
        count_awards_by_dim = A("reverse_nested", **{})
        award_count = A("cardinality", field="award_id")
        loan_value = A("sum", field="total_loan_amount")
        # Apply the aggregations
        search.aggs.bucket("aggs", aggs).bucket("group_by_dim_agg", group_by_dim_agg).metric(
            "dim_metadata", dim_metadata
        ).metric("sum_covid_obligation", sum_covid_obligation).metric("sum_covid_outlay", sum_covid_outlay).bucket(
            "count_awards_by_dim", count_awards_by_dim
        ).metric(
            "award_count", award_count
        ).metric(
            "sum_loan_value", loan_value
        )

        search.update_from_dict({"size": 0})

        return search

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
        buckets = response.get("aggs", {}).get("group_by_dim_agg", {}).get("buckets", [])
        totals = self.build_totals(buckets)

        results = self.build_elasticsearch_result(buckets)
        sorted_results = self.sort_results(results)

        return {"totals": totals, "results": sorted_results}

    def sort_results(self, results: List[dict]) -> List[dict]:
        sorted_parents = sorted(
            results, key=lambda val: val[self.pagination.sort_key], reverse=self.pagination.sort_order == "desc"
        )

        if self.has_children:
            for parent in sorted_parents:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val[self.pagination.sort_key],
                    reverse=self.pagination.sort_order == "desc",
                )

        return sorted_parents[self.pagination.lower_limit : self.pagination.upper_limit]
