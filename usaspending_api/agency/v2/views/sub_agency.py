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
from usaspending_api.references.models import Agency, SubtierAgency, Office
from usaspending_api.search.filters.elasticsearch.filter import QueryType


class SubAgencyList(PaginationMixin, AgencyBase):
    """
    Obtain the list of subagencies and offices based on the provided toptier_code and fiscal year, as well as award type.
    Can be either by funding agency or awarding agency.
    Based on transaction data.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_agency.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subtier_code_field = {"awarding": "awarding_sub_tier_agency_c", "funding": "funding_sub_tier_agency_co"}
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

        # Get the subtier codes
        subtier_codes = [bucket.get("key") for bucket in buckets if bucket.get("key") is not None]

        # Get the current recipient info
        current_subtier_info = {}
        subtier_info_query = SubtierAgency.objects.filter(subtier_code__in=subtier_codes).values(
            "subtier_code", "name", "abbreviation"
        )
        for subtier in subtier_info_query:
            current_subtier_info[subtier["subtier_code"]] = subtier

        # Get the office codes
        office_codes = []
        for bucket in buckets:
            office_codes.extend([child.get("key") for child in bucket.get("offices").get("buckets")])
        # remove any potential dups
        office_codes = set(list(office_codes))

        # Get the current recipient info
        current_office_info = {}
        for office in Office.objects.filter(office_code__in=office_codes).values("office_code", "office_name").all():
            current_office_info[office["office_code"]] = office["office_name"]

        for bucket in buckets:
            subtier_info = current_subtier_info[bucket.get("key")] or {}
            response.append(
                {
                    "abbreviation": subtier_info.get("abbreviation"),
                    "name": subtier_info.get("name"),
                    "total_obligations": round(bucket.get("total_subagency_obligations", {}).get("value", 0), 2),
                    "transaction_count": bucket.get("doc_count"),
                    "new_award_count": bucket.get("agency_award_count", {}).get("agency_award_value", {}).get("value"),
                    "children": sorted(
                        self.format_child_response(bucket.get("offices").get("buckets"), current_office_info),
                        key=lambda x: x.get(self.pagination.sort_key),
                        reverse=self.pagination.sort_order == "desc",
                    ),
                }
            )
        return response

    def format_child_response(self, children, current_office_info):
        response = []
        for child in children:
            response.append(
                {
                    "code": child.get("key"),
                    "name": current_office_info.get(child.get("key")),
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
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(
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
            "terms",
            field=f"{self.subtier_code_field[self.agency_type]}.keyword",
            size=term_agg_sizes["subtier_agency_size"],
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
            "terms", field=f"{self.subtier_code_field[self.agency_type]}.keyword", size=max_subtier_agencies
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
        max_office_count = resp_as_dict.get("max_office_count_agg", {}).get("value")

        # Default to Terms aggregation default of 10 to avoid parse error with size of 0
        return {"office_size": max_office_count or 10, "subtier_agency_size": max_subtier_agencies or 10}
