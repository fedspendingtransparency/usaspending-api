from django.db.models import Q, Sum, F
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations import file_b
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year


class ObjectClassList(PaginationMixin, AgencyBase):
    """
    Obtain the list of object classes for a specific agency in a single
    fiscal year based on whether or not that object class has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/object_class.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year", "filter"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "obligated_amount", "gross_outlay_amount"]
        self.default_sort_column = "obligated_amount"
        results = list(self.get_object_class_list())
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        results = results[self.pagination.lower_limit : self.pagination.upper_limit]
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "page_metadata": page_metadata,
                "results": results[: self.pagination.limit],
                "messages": self.standard_response_messages,
            }
        )

    def get_object_class_list(self) -> List[dict]:
        submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        filters = [
            Q(object_class__object_class_name__isnull=False),
            Q(submission_id__in=submission_ids),
            Q(treasury_account__funding_toptier_agency=self.toptier_agency),
            file_b.is_non_zero_total_spending(),
        ]
        if self.filter:
            filters.append(Q(object_class__object_class_name__icontains=self.filter))

        queryset_results = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("object_class__object_class_name")
            .annotate(
                name=F("object_class__object_class_name"),
                obligated_amount=Sum(file_b.get_obligations()),
                gross_outlay_amount=Sum(file_b.get_outlays()),
            )
            .order_by(f"{'-' if self.pagination.sort_order == 'desc' else ''}{self.pagination.sort_key}")
            .values("name", "obligated_amount", "gross_outlay_amount")
        )
        return queryset_results
