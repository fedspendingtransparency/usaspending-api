from typing import Any

from fiscalyear import FiscalYear
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters


class NewAwardCount(AgencyBase):
    """
    Obtain the count of new Awards under a specific Agency and a single Fiscal Year.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/awards/new/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type", "award_type_codes"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        count = self.new_awards_count()
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "agency_type": self.agency_type,
                "award_type_codes": self.award_type_codes,
                "new_award_count": count,
            }
        )

    def new_awards_count(self):
        fiscal_year = FiscalYear(self.fiscal_year)
        filters = {
            "award_type_codes": self.award_type_codes,
            "agencies": [{"type": self.agency_type, "tier": "toptier", "toptier_code": self.toptier_code}],
            "time_period": [{"start_date": fiscal_year.start.date(), "end_date": fiscal_year.end.date()}],
        }
        options = {"gte_field": "date_signed", "lte_field": "date_signed"}
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(filters, **options)
        search = AwardSearch().filter(filter_query)
        return search.handle_count()
