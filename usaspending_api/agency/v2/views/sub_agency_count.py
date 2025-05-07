from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from fiscalyear import FiscalYear
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType


class SubAgencyCount(PaginationMixin, AgencyBase):
    """
    Obtain the count of sub-agencies and offices for a given agency based on a toptier_code,
    fiscal_year, award_type, and agency_type (funding or awarding agency). This is based
    on transaction data.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_agency/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type", "award_type_codes"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        results = self.query_elasticsearch()
        formatted_results = self.format_elasticsearch_results(results)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "sub_agency_count": formatted_results["sub_agency_count"],
                "office_count": formatted_results["office_count"],
                "messages": self.standard_response_messages,
            }
        )

    def query_elasticsearch(self):
        fiscal_year = FiscalYear(self.fiscal_year)
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(
            {
                "agencies": [{"type": self.agency_type, "tier": "toptier", "name": self.toptier_agency.name}],
                "time_period": [{"start_date": fiscal_year.start.date(), "end_date": fiscal_year.end.date()}],
                "award_type_codes": self._query_params.get("award_type_codes", []),
            }
        )
        search = TransactionSearch().filter(filter_query)

        subtier_agency_agg = create_count_aggregation(f"{self.agency_type}_subtier_agency_name.keyword")
        office_agg = create_count_aggregation(f"{self.agency_type}_office_code.keyword")

        search.aggs.bucket("subtier_agencies", subtier_agency_agg)
        search.aggs.bucket("offices", office_agg)

        search.update_from_dict({"size": 0})
        response = search.handle_execute()
        return response

    def format_elasticsearch_results(self, results):
        sub_agencies = results.aggs.to_dict().get("subtier_agencies", {}).get("value", [])
        offices = results.aggs.to_dict().get("offices", {}).get("value", [])

        return {
            "sub_agency_count": sub_agencies,
            "office_count": offices,
        }
