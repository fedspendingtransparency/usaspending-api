from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from django.db.models import Q

from usaspending_api import settings
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.reporting.models import ReportingAgencyTas


class Differences(PaginationMixin, AgencyBase):
    """
    Obtain the differences between file A obligations and file B obligations for a specific agency/period
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/differences.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "fiscal_period"]
        self.additional_models = [
            {
                "key": "fiscal_year",
                "name": "fiscal_year",
                "type": "integer",
                "min": fy(settings.API_SEARCH_MIN_DATE),
                "max": current_fiscal_year(),
                "optional": False,
            },
            {
                "key": "fiscal_period",
                "name": "fiscal_period",
                "type": "integer",
                "min": 2,
                "max": 12,
                "optional": False,
            },
        ]

    def format_results(self, rows):
        order = self.pagination.sort_order == "desc"
        formatted_results = []
        for row in rows:
            formatted_results.append(
                {
                    "tas": row["tas_rendering_label"],
                    "file_a_obligation": row["appropriation_obligated_amount"],
                    "file_b_obligation": row["object_class_pa_obligated_amount"],
                    "difference": row["diff_approp_ocpa_obligated_amounts"],
                }
            )
        formatted_results = sorted(formatted_results, key=lambda x: x[self.pagination.sort_key], reverse=order)
        return formatted_results

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["difference", "file_a_obligation", "file_b_obligation", "tas"]
        self.default_sort_column = "tas"

        results = self.get_differences_queryset()
        formatted_results = self.format_results(results)
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        return Response(
            {
                "page_metadata": page_metadata,
                "results": formatted_results[self.pagination.lower_limit : self.pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_differences_queryset(self):
        filters = [
            Q(toptier_code=self.toptier_agency.toptier_code),
            Q(fiscal_year=self.fiscal_year),
            Q(fiscal_period=self.fiscal_period),
            ~Q(diff_approp_ocpa_obligated_amounts=0),
        ]
        results = (ReportingAgencyTas.objects.filter(*filters)).values(
            "tas_rendering_label",
            "appropriation_obligated_amount",
            "object_class_pa_obligated_amount",
            "diff_approp_ocpa_obligated_amounts",
        )
        return results
