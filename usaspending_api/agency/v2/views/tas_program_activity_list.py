from typing import Any, List

from django.db.models import Q, Sum, F
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year


class TASProgramActivityList(PaginationMixin, AgencyBase):
    """
    Obtain the list of program activities for a specific agency's treasury
    account using a treasury account symbol (TAS).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/treasury_account/tas/program_activity.md"

    file_b_calculations = FileBCalculations()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "filter"]
        self._submission_ids = None

    @property
    def tas_rendering_label(self):
        return self.kwargs["tas"]

    @property
    def submission_ids(self):
        if self._submission_ids is not None:
            return self._submission_ids
        self._submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        return self._submission_ids

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "obligated_amount", "gross_outlay_amount"]
        self.default_sort_column = "obligated_amount"
        results = list(self.get_program_activity_list())
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        pagination_limit_results = results[: self.pagination.limit]
        response_dict = {
            "treasury_account_symbol": self.tas_rendering_label,
            "fiscal_year": self.fiscal_year,
            "page_metadata": page_metadata,
            "results": pagination_limit_results,
            "messages": self.standard_response_messages,
        }
        # There isn't a direct relationship between RefProgramActivity and ObjectClass
        # That's why we opted to query for ObjectClass for each RefProgramActivity
        for idx, program_activity_row in enumerate(pagination_limit_results):
            object_class_results = self.get_object_class_by_program_activity_list(program_activity_row["name"])
            child_response_dict = self.format_object_class_children_response(object_class_results)
            response_dict["results"][idx]["children"] = child_response_dict

        return Response(response_dict)

    def get_program_activity_list(self) -> List[dict]:
        filters = [
            Q(program_activity__program_activity_name__isnull=False),
            Q(submission_id__in=self.submission_ids),
            Q(treasury_account__tas_rendering_label=self.tas_rendering_label),
            self.file_b_calculations.is_non_zero_total_spending(),
        ]
        if self.filter:
            filters.append(Q(program_activity__program_activity_name__icontains=self.filter))

        queryset_results = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("program_activity__program_activity_name")
            .annotate(
                name=F("program_activity__program_activity_name"),
                obligated_amount=Sum(self.file_b_calculations.get_obligations()),
                gross_outlay_amount=Sum(self.file_b_calculations.get_outlays()),
            )
            .order_by(f"{'-' if self.pagination.sort_order == 'desc' else ''}{self.pagination.sort_key}")
            .values("name", "obligated_amount", "gross_outlay_amount")
        )
        return queryset_results

    def get_object_class_by_program_activity_list(self, program_activity_name) -> List[dict]:
        filters = [
            Q(object_class__major_object_class_name__isnull=False),
            Q(submission_id__in=self.submission_ids),
            Q(program_activity__program_activity_name=program_activity_name),
            Q(treasury_account__tas_rendering_label=self.tas_rendering_label),
            self.file_b_calculations.is_non_zero_total_spending(),
        ]
        queryset_results = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("object_class__major_object_class_name")
            .annotate(
                name=F("object_class__major_object_class_name"),
                obligated_amount=Sum(self.file_b_calculations.get_obligations()),
                gross_outlay_amount=Sum(self.file_b_calculations.get_outlays()),
            )
            .order_by(f"{'-' if self.pagination.sort_order == 'desc' else ''}{self.pagination.sort_key}")
            .values("name", "obligated_amount", "gross_outlay_amount")
        )
        return queryset_results

    def format_object_class_children_response(self, children):
        response = []
        for child in children:
            response.append(
                {
                    "name": child.get("name"),
                    "obligated_amount": child.get("obligated_amount"),
                    "gross_outlay_amount": child.get("gross_outlay_amount"),
                }
            )
        return response
