from abc import abstractmethod
from typing import Dict, List, Optional, Union

from elasticsearch_dsl import A
from elasticsearch_dsl import Q as ES_Q
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
    nonzero_fields: Dict[str, str] = []
    query_fields: List[str]
    sub_agg_group_name: str = "sub_group_by_sub_agg_key"  # name used for the tier-2 aggregation group
    sub_agg_key: Union[str, None] = None  # will drive including of a sub-bucket-aggregation if overridden by subclasses
    sub_top_hits_fields: List[str]  # list used for top_hits sub aggregation
    top_hits_fields: List[str]  # list used for the top_hits aggregation

    pagination: Pagination  # Overwritten by a pagination mixin

    @cache_response()
    def post(self, request):
        return Response(self.perform_elasticsearch_search())

    def perform_elasticsearch_search(self, loans=False) -> Response:
        filters = {key: val for key, val in self.filters.items() if key != "award_type_codes"}
        if self.filters.get("award_type_codes") is not None:
            filters["award_type_codes"] = self.filters["award_type_codes"]
        # Need to update the value of "query" to have the fields to search on
        query_text = filters.pop("query", None)
        if query_text:
            filters["query"] = {"text": query_text, "fields": self.query_fields}

        # Ensure that Awards with File C records that cancel out are not taken into consideration;
        # The records that fall into this criteria share the same DEF Code and that is how the outlay and
        # obligation sum for the Award is able to be used even though the Award can have multiple DEFC
        filters["nonzero_fields"] = [
            "transaction_obligated_amount",
            "gross_outlay_amount_by_award_cpe",
            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe",
            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
        ]
        self.filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(filters)

        # using a set value here as doing an extra ES query is detrimental to performance
        # And the dimensions on which group-by aggregations are performed so far
        # (agency, TAS, object_class) all have cardinality less than this number
        # If the data increases to a point where there are more results than this, it should be changed
        self.bucket_count = 1000
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
        if "query" in self.filters:
            terms = ES_Q("terms", **{"disaster_emergency_fund_code": self.filters.get("def_codes")})
            query = ES_Q(
                "multi_match",
                query=self.filters["query"],
                type="phrase_prefix",
                fields=[f"{query}" for query in self.query_fields],
            )
            filter_agg_query = ES_Q("bool", should=[terms, query], minimum_should_match=2)
        else:
            filter_agg_query = ES_Q("terms", **{"disaster_emergency_fund_code.keyword": self.filters.get("def_codes")})
        filtered_aggs = A("filter", filter_agg_query)
        group_by_dim_agg = A("terms", field=self.agg_key, size=self.bucket_count)
        # Group the FABA records by their Award key
        # This is done since the FABA records are no longer nested under their parent Award in the same document
        group_by_awards_agg = A("terms", field="financial_account_distinct_award_key.keyword")
        dim_metadata = A(
            "top_hits",
            size=1,
            sort=[{"update_date": {"order": "desc"}}],
            _source={"includes": self.top_hits_fields},
        )
        # Fields to include for each Award
        award_metadata = A("top_hits", size=1, _source={"includes": "total_loan_value"})
        sum_covid_outlay = A(
            "sum",
            script="""doc['is_final_balances_for_fy'].value ? (
             ( doc['gross_outlay_amount_by_award_cpe'].size() > 0 ? doc['gross_outlay_amount_by_award_cpe'].value : 0)
              + (doc['ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].size() > 0 ? doc['ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].value : 0)
              + (doc['ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].size() > 0 ? doc['ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].value : 0) ) : 0""",
        )
        sum_covid_obligation = A("sum", field="transaction_obligated_amount")

        # Apply the aggregations
        search.aggs.bucket("filtered_aggs", filtered_aggs).bucket("group_by_dim_agg", group_by_dim_agg).metric(
            "dim_metadata", dim_metadata
        ).metric("sum_transaction_obligated_amount", sum_covid_obligation).metric(
            "sum_gross_outlay_amount_by_award_cpe", sum_covid_outlay
        ).bucket(
            "group_by_awards", group_by_awards_agg
        ).bucket(
            "award_metadata", award_metadata
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
            sort=[{"update_date": {"order": "desc"}}],
            _source={"includes": self.sub_top_hits_fields},
        )
        sub_sum_covid_outlay = A(
            "sum",
            script="""doc['is_final_balances_for_fy'].value ? ( ( doc['gross_outlay_amount_by_award_cpe'].size() > 0 ? doc['gross_outlay_amount_by_award_cpe'].value : 0)
             + (doc['ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].size() > 0 ? doc['ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe'].value : 0)
             + (doc['ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].size() > 0 ? doc['ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe'].value : 0) ) : 0""",
        )
        sub_sum_covid_obligation = A("sum", field="transaction_obligated_amount")
        sub_count_awards_by_dim = A("reverse_nested", **{})
        sub_award_count = A("value_count", field="financial_account_distinct_award_key")
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

    def build_totals(self, response: List[dict], loans: bool = False) -> dict:
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
            totals = self.build_totals(response=[], loans=loans)
            return {"totals": totals, "results": []}
        response = search.handle_execute()
        response = response.aggs.to_dict()
        buckets = response.get("filtered_aggs", {}).get("group_by_dim_agg", {}).get("buckets", [])
        results = self.build_elasticsearch_result(buckets)
        totals = self.build_totals(results, loans)
        sorted_results = self.sort_results(results)
        return {"totals": totals, "results": sorted_results}

    def sort_results(self, results: List[dict]) -> List[dict]:
        # Add a sort specifically for agency names so that the agency names that contain "of the" are
        #   sorted correctly and not just at the end of the list of agencies
        if self.pagination.sort_key == "description":
            sorted_parents = sorted(
                results,
                key=lambda val: val.get("description", "id").lower(),
                reverse=self.pagination.sort_order == "desc",
            )
        else:
            sorted_parents = sorted(
                results,
                key=lambda val: val.get(self.pagination.sort_key, "id"),
                reverse=self.pagination.sort_order == "desc",
            )

        if self.has_children:
            for parent in sorted_parents:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val.get(self.pagination.sort_key, "id"),
                    reverse=self.pagination.sort_order == "desc",
                )

        return sorted_parents
