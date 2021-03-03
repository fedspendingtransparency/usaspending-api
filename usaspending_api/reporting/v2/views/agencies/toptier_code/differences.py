from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from django.db.models import Q

from usaspending_api.agency.v2.views.agency_base import AgencyBase

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns
from usaspending_api.references.models import ToptierAgency
from usaspending_api.reporting.models import ReportingAgencyTas


class Differences(AgencyBase):
    """
    Obtain the differences between file A obligations and file B obligations for a specific agency/period
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/differences.md"

    @staticmethod
    def _parse_and_validate_request(request_dict) -> dict:
        sortable_columns = ["difference", "file_a_obligation", "file_b_obligation", "tas"]
        default_sort_column = "tas"
        models = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        models.extend(
            [
                {"key": "fiscal_year", "name": "fiscal_year", "type": "integer", "optional": False},
                {"key": "fiscal_period", "name": "fiscal_period", "type": "integer", "optional": False},
            ]
        )

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    @staticmethod
    def format_results(rows, pagination):
        order = pagination.sort_order == "desc"
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
        formatted_results = sorted(formatted_results, key=lambda x: x[pagination.sort_key], reverse=order)
        return formatted_results

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        request_data = self._parse_and_validate_request(request.query_params)

        toptier_agency = (
            ToptierAgency.objects.account_agencies(agency_type="awarding")
            .filter(toptier_code=self.toptier_code)
            .first()
        )
        if not toptier_agency:
            raise NotFound(f"Agency with a toptier code of '{self.toptier_code}' does not exist")
        request_data["toptier_agency"] = toptier_agency
        self.validate_fiscal_period(request_data)
        pagination = Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", "tas"),
            sort_order=request_data["order"],
        )
        results = self.get_differences_queryset(request_data)
        formatted_results = self.format_results(results, pagination)
        page_metadata = get_pagination_metadata(len(results), pagination.limit, pagination.page)
        return Response(
            {
                "page_metadata": page_metadata,
                "results": formatted_results[pagination.lower_limit : pagination.upper_limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_differences_queryset(self, request_data):
        filters = [
            Q(toptier_code=request_data["toptier_agency"].toptier_code),
            Q(fiscal_year=self.fiscal_year),
            Q(fiscal_period=request_data["fiscal_period"]),
            ~Q(diff_approp_ocpa_obligated_amounts=0),
        ]
        results = (ReportingAgencyTas.objects.filter(*filters)).values(
            "tas_rendering_label",
            "appropriation_obligated_amount",
            "object_class_pa_obligated_amount",
            "diff_approp_ocpa_obligated_amounts",
        )
        return results
