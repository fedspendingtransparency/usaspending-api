from enum import Enum
from typing import Any, List


from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod
from django.conf import settings
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from elasticsearch_dsl import Q
from usaspending_api.common.helpers.fiscal_year_helpers import (
    get_fiscal_year_end_datetime,
    get_fiscal_year_start_datetime,
)


class GroupEnum(Enum):
    ALL = "all"
    CFO = "cfo"


class AwardCount(PaginationMixin, AgencyBase):
    """
    Obtain a count of Awards grouped by Award Type under Agencies
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/awards/count.md"

    def __init__(self, *args, **kwargs):
        self.additional_models = [
            {
                "key": "group",
                "name": "group",
                "type": "enum",
                "enum_values": ["all", "cfo"],
                "optional": True,
                "default": "all",
            },
        ]
        self.sortable_columns = ["awarding_toptier_agency_name.keyword"]
        self.default_sort_column = "awarding_toptier_agency_name.keyword"
        self.params_to_validate = ["fiscal_year", "group"]
        super().__init__(*args, **kwargs)
        # The following are awarding toptier agency names
        # that will be the only agencies returned when the
        # cfo filter is used
        self._toptier_cfo_agency_names = [
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

    @property
    def group(self):
        return self._query_params.get("group")

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self._fy_end = get_fiscal_year_end_datetime(self.fiscal_year).date()
        self._fy_start = get_fiscal_year_start_datetime(self.fiscal_year).date()
        results = self.query_elasticsearch_for_prime_awards({})
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = (results[self.pagination.lower_limit : self.pagination.upper_limit],)
        return Response(
            {
                "results": results[: self.pagination.limit],
                "page_metadata": page_metadata,
                "messages": self.standard_response_messages,
            }
        )

    def query_elasticsearch_for_prime_awards(self, filters) -> List:
        filter_options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=QueryType.AWARDS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        filters.update({"time_period": [{"start_date": self._fy_start, "end_date": self._fy_end}]})
        if self.group == GroupEnum.CFO.value:
            filters.update(
                {
                    "agencies": [
                        {"type": "awarding", "tier": "toptier", "name": awarding_toptier_agency_name}
                        for awarding_toptier_agency_name in self._toptier_cfo_agency_names
                    ]
                }
            )
        agency_award_count_results = self._get_award_counts_response(filters, filter_options)
        return agency_award_count_results

    def _get_award_counts_response(self, filters: List[dict], filter_options: dict) -> List[dict]:
        """Returns counts of awards filtered by the filter parameters.

        Args:
            filters: Filters to filter data on
            filter_options: Additional filters to apply

        Returns:
            List[dict]: A dictionary containing award types and the number of their awards.
        """
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters, **filter_options)
        record_num = (self.pagination.page - 1) * self.pagination.limit
        sorts = [{self.default_sort_column: self.pagination.sort_order}]
        s = AwardSearch().filter(filter_query).sort(*sorts)[record_num : record_num + self.pagination.limit]

        s.aggs.bucket("agencies", "terms", field="awarding_toptier_agency_name.keyword", size=999999)
        s.aggs["agencies"].bucket("codes", "terms", field="awarding_toptier_agency_code.keyword", size=999999)
        s.aggs["agencies"].bucket(
            "types",
            "filters",
            filters={category: Q("terms", type=types) for category, types in all_award_types_mappings.items()},
        )
        results = s.handle_execute()

        response = []
        for agency_bucket_element in results.aggregations.agencies.buckets:
            for agency_code_bucket_element in agency_bucket_element.codes.buckets:
                agency_code = agency_code_bucket_element.key
            contracts = agency_bucket_element.types.buckets.contracts.doc_count
            idvs = agency_bucket_element.types.buckets.idvs.doc_count
            grants = agency_bucket_element.types.buckets.grants.doc_count
            direct_payments = agency_bucket_element.types.buckets.direct_payments.doc_count
            loans = agency_bucket_element.types.buckets.loans.doc_count
            other = agency_bucket_element.types.buckets.other_financial_assistance.doc_count
            agency_response = {
                "awarding_toptier_agency_name": agency_bucket_element.key,
                "awarding_toptier_agency_code": agency_code,
                "contracts": contracts,
                "direct_payments": direct_payments,
                "grants": grants,
                "idvs": idvs,
                "loans": loans,
                "other": other,
            }
            response.append(agency_response)
        return response
