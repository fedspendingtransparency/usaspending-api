from elasticsearch_dsl import A
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from fiscalyear import FiscalYear
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.references.models import Agency


class SubAgencyList(PaginationMixin, AgencyBase):
    """
    Obtain the list of subagencies and offices based on the provided toptier_code and fiscal year, as well as award type.
    Can be either by funding agency or awarding agency.
    Based on transaction data.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_agency.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type", "award_type_codes"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "total_obligations", "transaction_count", "new_award_count"]
        self.default_sort_column = "total_obligations"
        self.filter_query = self.build_elasticsearch_filter_query()
        results = sorted(
            self.get_sub_agency_list(),
            key=lambda x: x.get(self.pagination.sort_key),
            reverse=self.pagination.sort_order == "desc",
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "page_metadata": page_metadata,
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def format_elasticsearch_results(self, results):
        response = []
        buckets = results.aggs.to_dict().get("subtier_agencies", {}).get("buckets", [])
        for bucket in buckets:
            response.append(
                {
                    "name": bucket.get("key"),
                    "abbreviation": bucket.get("subagency_info", {})
                    .get("hits", {})
                    .get("hits", [{}])[0]
                    .get("_source", {})
                    .get(f"{self.agency_type}_subtier_agency_abbreviation"),
                    "total_obligations": round(bucket.get("total_subagency_obligations", {}).get("value", 0), 2),
                    "transaction_count": bucket.get("doc_count"),
                    "new_award_count": bucket.get("agency_award_count", {}).get("agency_award_value", {}).get("value"),
                    "children": sorted(
                        self.format_child_response(bucket.get("offices").get("buckets")),
                        key=lambda x: x.get(self.pagination.sort_key),
                        reverse=self.pagination.sort_order == "desc",
                    ),
                }
            )
        return response

    def format_child_response(self, children):
        response = []
        for child in children:
            response.append(
                {
                    "name": child.get("office_info", {})
                    .get("hits", {})
                    .get("hits", [{}])[0]
                    .get("_source", {})
                    .get(f"{self.agency_type}_office_name"),
                    "code": child.get("key"),
                    "total_obligations": round(child.get("total_office_obligations", {}).get("value", 0), 2),
                    "transaction_count": child.get("doc_count"),
                    "new_award_count": child.get("office_award_count", {}).get("office_award_value", {}).get("value"),
                }
            )
        return response

    def get_sub_agency_list(self):
        response = self.query_elasticsearch()
        return self.format_elasticsearch_results(response)

    def build_elasticsearch_filter_query(self):
        fiscal_year = FiscalYear(self.fiscal_year)
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(
            {
                "agencies": [{"type": self.agency_type, "tier": "toptier", "name": self.toptier_agency.name}],
                "time_period": [{"start_date": fiscal_year.start.date(), "end_date": fiscal_year.end.date()}],
                "award_type_codes": self._query_params.get("award_type_codes", []),
            }
        )
        return filter_query

    def query_elasticsearch(self):
        fiscal_year = FiscalYear(self.fiscal_year)
        search = TransactionSearch().filter(self.filter_query)
        term_agg_sizes = self.get_term_agg_size_values()
        subagency_dim_metadata = A(
            "top_hits",
            size=1,
            _source={"includes": [f"{self.agency_type}_subtier_agency_abbreviation"]},
        )
        office_dim_metadata = A(
            "top_hits",
            size=1,
            _source={"includes": [f"{self.agency_type}_office_name"]},
        )
        subtier_agency_agg = A(
            "terms", field=f"{self.agency_type}_subtier_agency_name.keyword", size=term_agg_sizes["subtier_agency_size"]
        )
        office_agg = A("terms", field=f"{self.agency_type}_office_code.keyword", size=term_agg_sizes["office_size"])
        agency_obligation_agg = A("sum", field="generated_pragmatic_obligation")
        office_obligation_agg = A("sum", field="generated_pragmatic_obligation")
        new_award_filter = A(
            "filter", range={"award_date_signed": {"gte": fiscal_year.start.date(), "lte": fiscal_year.end.date()}}
        )
        agency_new_award_agg = create_count_aggregation("award_id")
        office_new_award_agg = create_count_aggregation("award_id")

        search.aggs.bucket("subtier_agencies", subtier_agency_agg).metric(
            "total_subagency_obligations", agency_obligation_agg
        ).metric("subagency_info", subagency_dim_metadata).bucket("offices", office_agg).metric(
            "total_office_obligations", office_obligation_agg
        ).metric(
            "office_info", office_dim_metadata
        ).bucket(
            "office_award_count", new_award_filter
        ).metric(
            "office_award_value", office_new_award_agg
        )

        search.aggs["subtier_agencies"].bucket(
            "agency_award_count",
            A("filter", range={"award_date_signed": {"gte": fiscal_year.start.date(), "lte": fiscal_year.end.date()}}),
        ).metric("agency_award_value", agency_new_award_agg)
        search.update_from_dict({"size": 0})
        response = search.handle_execute()
        return response

    def get_term_agg_size_values(self):
        search = TransactionSearch().filter(self.filter_query)
        max_subtier_agencies = Agency.objects.filter(
            toptier_agency__toptier_code=self.toptier_code, subtier_agency__isnull=False
        ).count()

        unique_subtier_agg = A(
            "terms", field=f"{self.agency_type}_subtier_agency_name.keyword", size=max_subtier_agencies + 100
        )
        unique_office_count_agg = A("cardinality", field=f"{self.agency_type}_office_code.keyword")
        max_office_count_agg = A("max_bucket", buckets_path="unique_subtier_agg>unique_office_count_agg")

        search.aggs.bucket("unique_subtier_agg", unique_subtier_agg).metric(
            "unique_office_count_agg", unique_office_count_agg
        )
        search.aggs.bucket("max_office_count_agg", max_office_count_agg)
        search.update_from_dict({"size": 0})
        response = search.handle_execute()
        resp_as_dict = response.aggs.to_dict()
        max_office_count = resp_as_dict.get("max_office_count_agg", {}).get("value", 10)
        return {"office_size": max_office_count, "subtier_agency_size": max_subtier_agencies}
