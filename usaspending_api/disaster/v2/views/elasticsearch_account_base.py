from abc import abstractmethod
from typing import List, Optional, Dict

from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AccountSearch
from usaspending_api.common.exceptions import ForbiddenException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase


class ElasticsearchAccountDisasterBase(DisasterBase):
    agg_group_name: str = "group_by_agg_key"  # name used for the tier-1 aggregation group
    agg_key: str
    bucket_count: int
    filter_query: ES_Q
    has_children: bool = False
    nested_nonzero_fields: Dict[str, str] = []
    nonzero_fields: Dict[str, str] = []
    query_fields: List[str]
    sub_agg_group_name: str = "sub_group_by_sub_agg_key"  # name used for the tier-2 aggregation group
    sub_agg_key: str = None  # will drive including of a sub-bucket-aggregation if overridden by subclasses
    sub_top_hits_fields: List[str]  # list used for top_hits sub aggregation
    top_hits_fields: List[str]  # list used for the top_hits aggregation

    pagination: Pagination  # Overwritten by a pagination mixin

    @cache_response()
    def post(self, request):
        return Response(self.perform_elasticsearch_search())

    def perform_elasticsearch_search(self, loans=False) -> Response:
        filters = {f"nested_{key}": val for key, val in self.filters.items() if key != "award_type"}
        if self.filters.get("award_type") is not None:
            filters["award_type"] = self.filters["award_type"]
        # Need to update the value of "query" to have the fields to search on
        query = filters.pop("nested_query", None)
        if query:
            filters["nested_query"] = {"text": query, "fields": self.query_fields}

        # Ensure that only non-zero values are taken into consideration
        filters["nested_nonzero_fields"] = list(self.nested_nonzero_fields.values())
        filters["nonzero_fields"] = self.nonzero_fields
        self.filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(filters)

        self.bucket_count = 1000  # get_number_of_unique_terms_for_accounts(self.filter_query, self.agg_key)
        messages = []
        if self.pagination.sort_key in ("id", "code"):
            messages.append(
                (
                    f"Notice! API Request to sort on '{self.pagination.sort_key}' field isn't fully implemented."
                    " Results were actually sorted using 'description' field."
                )
            )

        response = self.query_elasticsearch(loans)
        response["page_metadata"] = get_pagination_metadata(
            len(response["results"]), self.pagination.limit, self.pagination.page
        )
        response["results"] = response["results"][self.pagination.lower_limit : self.pagination.upper_limit]
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
        # No need to continue if there is no result
        if self.bucket_count == 0:
            return None

        # Create the initial search using filters
        search = AccountSearch().filter(self.filter_query)

        # Create the aggregations
        financial_accounts_agg = A("nested", path="financial_accounts_by_award")
        group_by_dim_agg = A("terms", field=self.agg_key, size=self.bucket_count)
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
        award_count = A("value_count", field="award_id")
        loan_value = A("sum", field="total_loan_value")

        # Apply the aggregations
        search.aggs.bucket(self.agg_group_name, financial_accounts_agg).bucket(
            "group_by_dim_agg", group_by_dim_agg
        ).metric("dim_metadata", dim_metadata).metric("sum_transaction_obligated_amount", sum_covid_obligation).metric(
            "sum_gross_outlay_amount_by_award_cpe", sum_covid_outlay
        ).bucket(
            "count_awards_by_dim", count_awards_by_dim
        ).metric(
            "award_count", award_count
        ).metric(
            "sum_loan_value", loan_value
        )

        # Apply sub-aggregation for children if applicable
        if self.sub_agg_key:
            self.extend_elasticsearch_search_with_sub_aggregation(search)

        search.update_from_dict({"size": 0})

        return search

    def extend_elasticsearch_search_with_sub_aggregation(self, search: AccountSearch):
        """
        This template method is called if the `self.sub_agg_key` is supplied, in order to post-process the query and
        inject a sub-aggregation on a secondary dimension (that is subordinate to the first agg_key's dimension).

        Example: Subtier Agency spending rolled up to Toptier Agency spending
        """
        sub_bucket_count = 1000  # get_number_of_unique_terms_for_accounts(self.filter_query, f"{self.sub_agg_key}")
        size = sub_bucket_count
        shard_size = sub_bucket_count + 100

        if shard_size > 10000:
            raise ForbiddenException(
                "Current filters return too many unique items. Narrow filters to return results or use downloads."
            )

        # Sub-aggregation to append to primary agg
        sub_group_by_sub_agg_key_values = {"field": self.sub_agg_key, "size": size, "shard_size": shard_size}

        sub_group_by_sub_agg_key = A("terms", **sub_group_by_sub_agg_key_values)
        sub_dim_metadata = A(
            "top_hits",
            size=1,
            sort=[{"financial_accounts_by_award.update_date": {"order": "desc"}}],
            _source={"includes": self.sub_top_hits_fields},
        )
        sub_sum_covid_outlay = A(
            "sum",
            field="financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
            script={"source": "doc['financial_accounts_by_award.is_final_balances_for_fy'].value ? _value : 0"},
        )
        sub_sum_covid_obligation = A("sum", field="financial_accounts_by_award.transaction_obligated_amount")
        sub_count_awards_by_dim = A("reverse_nested", **{})
        sub_award_count = A("value_count", field="award_id")
        loan_value = A("sum", field="total_loan_value")

        sub_group_by_sub_agg_key.metric("dim_metadata", sub_dim_metadata).metric(
            "sum_transaction_obligated_amount", sub_sum_covid_obligation
        ).metric("sum_gross_outlay_amount_by_award_cpe", sub_sum_covid_outlay).bucket(
            "count_awards_by_dim", sub_count_awards_by_dim
        ).metric(
            "award_count", sub_award_count
        ).metric(
            "sum_loan_value", loan_value
        )

        # Append sub-agg to primary agg, and include the sub-agg's sum metric aggs too
        search.aggs[self.agg_group_name]["group_by_dim_agg"].bucket(self.sub_agg_group_name, sub_group_by_sub_agg_key)

    def build_totals(self, response: List[dict], loans: bool) -> dict:
        obligations = 0
        outlays = 0
        award_count = 0
        loan_sum = 0
        for item in response:
            obligations += item["obligation"]
            outlays += item["outlay"]
            award_count += item["award_count"]
            if loans:
                loan_sum += item["face_value_of_loan"]

        retval = {"obligation": round(obligations, 2), "outlay": round(outlays, 2), "award_count": award_count}
        if loans:
            retval["face_value_of_loan"] = loan_sum
        return retval

    def query_elasticsearch(self, loans) -> dict:
        search = self.build_elasticsearch_search_with_aggregations()
        if search is None:
            totals = self.build_totals(response=[])
            return {"totals": totals, "results": []}

        response = search.handle_execute()
        response = response.aggs.to_dict()
        buckets = response.get(self.agg_group_name, {}).get("group_by_dim_agg", {}).get("buckets", [])

        results = self.build_elasticsearch_result(buckets)
        totals = self.build_totals(results, loans)
        sorted_results = self.sort_results(results)
        return {"totals": totals, "results": sorted_results}

    def sort_results(self, results: List[dict]) -> List[dict]:
        sorted_parents = sorted(
            results,
            key=lambda val: val.get(self.pagination.sort_key, "id"),
            reverse=self.pagination.sort_order == "desc",
        )

        if self.has_children:
            for parent in sorted_parents:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val(self.pagination.sort_key, "id"),
                    reverse=self.pagination.sort_order == "desc",
                )

        return sorted_parents
