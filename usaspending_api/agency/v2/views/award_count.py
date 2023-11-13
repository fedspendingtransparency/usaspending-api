from typing import Any


from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.references.models.toptier_agency import ToptierAgency
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from django.conf import settings
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from elasticsearch_dsl import Q
from usaspending_api.common.helpers.fiscal_year_helpers import (
    get_fiscal_year_end_datetime,
    get_fiscal_year_start_datetime,
)


class AwardCount(AgencyBase):
    """
    Obtain the count of awards for a specific agency in a single fiscal year.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/awards/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year"]
        self._toptier_awarding_agencies_to_include = [
            "Agency for International Development",
            "Department of Agriculture",
            "Department of Commerce",
            "Department of Defense",
            "Department of Education",
            "Department of Energy",
            "Department of Health and Human Services",
            "Department of Homeland Security",
            "Department of Housing and Urban Development",
            "Department of the Interior",
            "Department of Justice",
            "Department of Labor",
            "Department of State",
            "Department of Transportation",
            "Department of the Treasury",
            "Department of Veterans Affairs",
            "Environmental Protection Agency",
            "General Services Administration",
            "National Aeronautics and Space Administration",
            "National Science Foundation",
            "Nuclear Regulatory Commission",
            "Office of Personnel Management",
            "Small Business Administration",
            "Social Security Administration",
        ]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self._fy_end = get_fiscal_year_end_datetime(self.fiscal_year).date()
        self._fy_start = get_fiscal_year_start_datetime(self.fiscal_year).date()

        results = self.query_elasticsearch_for_prime_awards({})

        raw_response = {"results": results, "messages": []}

        return Response(raw_response)

    def _generate_filters(self, awarding_toptier_agency_name):
        return {
            "agencies": [{"type": "awarding", "tier": "toptier", "name": awarding_toptier_agency_name}],
            "time_period": [{"start_date": self._fy_start, "end_date": self._fy_end}],
        }

    def query_elasticsearch_for_prime_awards(self, filters) -> list:
        toptier_awarding_agencies_to_include_results = []
        filter_options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=_QueryType.AWARDS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        for awarding_toptier_agency_name in self._toptier_awarding_agencies_to_include:
            awarding_toptier_agency_code = ToptierAgency.objects.filter(name=awarding_toptier_agency_name).first().toptier_code
            filters.update(self._generate_filters(awarding_toptier_agency_name))
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
                "awarding_toptier_agency_name": awarding_toptier_agency_name,
                "awarding_toptier_agency_code": awarding_toptier_agency_code,
                "contracts": contracts,
                "direct_payments": direct_payments,
                "grants": grants,
                "idvs": idvs,
                "loans": loans,
                "other": other,
            }
            toptier_awarding_agencies_to_include_results.append(response)
        return toptier_awarding_agencies_to_include_results
