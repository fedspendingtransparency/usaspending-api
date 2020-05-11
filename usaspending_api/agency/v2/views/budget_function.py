from django.db.models import Q, Sum
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class BudgetFunction(AgencyBase):
    """
    Obtain the count of budget functions for a specific agency in a single
    fiscal year based on whether or not that budget function has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/budget_function/id.md"

    def validate_request(self):
        return None

    def combine_subfunctions(self, rows):
        budget_functions = [
            {
                "code": row["treasury_account__budget_function_code"],
                "name": row["treasury_account__budget_function_title"],
            }
            for row in rows
        ]
        for item in budget_functions:
            item["children"] = [
                {
                    "code": row["treasury_account__budget_subfunction_code"],
                    "name": row["treasury_account__budget_subfunction_title"],
                    "obligated_amount": row["obligated_amount"],
                    "gross_outlay_amount": row["gross_outlay_amount"],
                }
                for row in rows
                if item["code"] == row["treasury_account__budget_function_code"]
            ]
        for item in budget_functions:
            item["obligated_amount"] = sum([x["obligated_amount"] for x in item["children"]])
            item["gross_outlay_amount"] = sum([x["gross_outlay_amount"] for x in item["children"]])
        return budget_functions

    @cache_response()
    def post(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        rows = list(self.get_budget_function_queryset(request))

        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "budget_functions": self.combine_subfunctions(rows),
                "messages": self.standard_response_messages,
            }
        )

    def get_budget_function_queryset(self, request):
        filters = [~Q(treasury_account__budget_function_code=""), ~Q(treasury_account__budget_function_code=None)]
        if request.data.get("filter") is not None:
            filters.append(
                Q(
                    Q(treasury_account__budget_function_title__icontains=request.data["filter"])
                    | Q(treausry_account__budget_subfunction_title__icontains=request.data["filter"])
                )
            )

        results = (
            (
                FinancialAccountsByProgramActivityObjectClass.objects.filter(
                    final_of_fy=True,
                    treasury_account__funding_toptier_agency=self.toptier_agency,
                    submission__reporting_fiscal_year=self.fiscal_year,
                )
                .exclude(
                    obligations_incurred_by_program_object_class_cpe=0,
                    gross_outlay_amount_by_program_object_class_cpe=0,
                )
                .filter(*filters)
            )
            .values(
                "treasury_account__budget_function_code",
                "treasury_account__budget_function_title",
                "treasury_account__budget_subfunction_code",
                "treasury_account__budget_subfunction_title",
            )
            .annotate(
                obligated_amount=Sum("obligations_incurred_by_program_object_class_cpe"),
                gross_outlay_amount=Sum("gross_outlay_amount_by_program_object_class_cpe"),
            )
        )
        return results
