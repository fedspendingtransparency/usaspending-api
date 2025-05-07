from typing import Any

from fiscalyear import FiscalYear
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from django.conf import settings


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
            "agencies": [{"type": self.agency_type, "tier": "toptier", "toptier_code": self.toptier_code}],
            "time_period": [
                {
                    "start_date": fiscal_year.start.date(),
                    "end_date": fiscal_year.end.date(),
                    "date_type": "new_awards_only",
                }
            ],
        }
        if self.award_type_codes:
            filters["award_type_codes"] = self.award_type_codes

        filter_options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=QueryType.AWARDS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters, **filter_options)
        search = AwardSearch().filter(filter_query)
        return search.handle_count()
