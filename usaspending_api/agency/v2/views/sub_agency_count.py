from elasticsearch_dsl import A
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from fiscalyear import FiscalYear
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters


class SubAgencyCount(PaginationMixin, AgencyBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_agency/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type", "award_type_codes"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        results = self.query_elasticsearch(),
        formatted_results = self.format_elasticsearch_results(results)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "messages": self.standard_response_messages,
                **formatted_results
            }
        )

    def query_elasticsearch(self):
        fiscal_year = FiscalYear(self.fiscal_year)
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(
            {
                "agencies": [{"type": self.agency_type, "tier": "toptier", "name": self.toptier_agency.name}],
                "time_period": [{"start_date": fiscal_year.start.date(), "end_date": fiscal_year.end.date()}],
                "award_type_codes": self._query_params.get("award_type_codes", []),
            }
        )
        search = TransactionSearch().filter(filter_query)

        subtier_agency_agg = A("terms", field=f"{self.agency_type}_subtier_agency_name.keyword")
        office_agg = A("terms", field=f"{self.agency_type}_office_code.keyword")

        search.aggs.bucket("subtier_agencies", subtier_agency_agg).bucket("offices", office_agg)
        search.aggs.bucket("offices", office_agg)

        response = search.handle_execute()
        return response

    def format_elasticsearch_results(self, results):
        sub_agencies = results[0].aggregations.to_dict().get("subtier_agencies", {}).get("buckets", [])
        offices = results[0].aggregations.to_dict().get("offices", {}).get("buckets", [])

        office_count_breakdown = 0
        office_breakdown = []
        for sub_agency in sub_agencies:
            office_count_breakdown += len(sub_agency["offices"]["buckets"])
            office_breakdown += sub_agency["offices"]["buckets"]

        office_breakdown = list(map(lambda office: office["key"], office_breakdown))
        offices = list(map(lambda office: office["key"], offices))

        office_diff = list(set(office_breakdown) - set(offices))

        return {
            "sub_agencies": sub_agencies,
            "sub_agency_count": len(sub_agencies),
            "office_count_breakdown": office_count_breakdown,
            "office_count": len(offices),
            "offices_breakdown": office_breakdown,
            "offices": offices,
            "office_diff": office_diff
        }
