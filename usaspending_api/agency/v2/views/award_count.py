from typing import Any

from fiscalyear import FiscalYear
from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from django.conf import settings
from django.db.models import Count
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from elasticsearch_dsl import Q

class AwardCount(AgencyBase):
    """
    Obtain the count of awards for a specific agency in a single fiscal year
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/award_count/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "agency_type"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        # TODO validate fiscal year
        # TODO: move this to the fiscal year helper module
        fiscal_year = FiscalYear(self.fiscal_year)
        self._fy_end = fiscal_year.end.date()
        self._fy_start = fiscal_year.start.date()
        filters = self._generate_filters()
        # TODO: filter response by toptier agency code
        award_types_counted = {"contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0, "subcontracts": 0, "subgrants": 0}
        # award_types_counted.update(self.handle_subawards(filters))
        award_types_counted.update(self.query_elasticsearch_for_prime_awards(filters))

        raw_response = {
            "results": award_types_counted,
            "messages": []
        }

        return Response(raw_response)

    def _generate_filters(self):
        return {
            "agencies": [{"type": self.agency_type, "tier": "toptier", "toptier_code": self.toptier_code}],
            "time_period": [{"start_date": self._fy_start, "end_date": self._fy_end}],
        }

    @staticmethod
    def handle_subawards(filters: dict) -> dict:
        """Turn the filters into the result dictionary when dealing with Sub-Awards

        Note: Due to how the Django ORM joins to the awards table as an
        INNER JOIN, it is necessary to explicitly enforce the aggregations
        to only count Sub-Awards that are linked to a Prime Award.

        Remove the filter and update if we can move away from this behavior.
        """
        queryset = (
            subaward_filter(filters)
            .filter(award_id__isnull=False)
            .values("prime_award_group")
            .annotate(count=Count("broker_subaward_id"))
        )

        results = {}
        results["subgrants"] = sum([sub["count"] for sub in queryset if sub["prime_award_group"] == "grant"])
        results["subcontracts"] = sum([sub["count"] for sub in queryset if sub["prime_award_group"] == "procurement"])

        return results

    def query_elasticsearch_for_prime_awards(self, filters) -> list:
        filter_options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=_QueryType.AWARDS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(filters, **filter_options)
        s = AwardSearch().filter(filter_query)

        s.aggs.bucket(
            "types",
            "filters",
            filters={category: Q("terms", type=types) for category, types in all_award_types_mappings.items()},
        )
        results = s.handle_execute()

        contracts = results.aggregations.types.buckets.contracts.doc_count
        idvs = results.aggregations.types.buckets.idvs.doc_count
        grants = results.aggregations.types.buckets.grants.doc_count
        direct_payments = results.aggregations.types.buckets.direct_payments.doc_count
        loans = results.aggregations.types.buckets.loans.doc_count
        other = results.aggregations.types.buckets.other_financial_assistance.doc_count

        response = {
            "contracts": contracts,
            "direct_payments": direct_payments,
            "grants": grants,
            "idvs": idvs,
            "loans": loans,
            "other": other,
        }
        return response
