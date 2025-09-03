from django.db.models import Case, Q, TextField, When, Value, Sum
from django.db.models.functions import Coalesce
from rest_framework.response import Response
from rest_framework.request import Request

from usaspending_api.accounts.v2.views.federal_account_base import FederalAccountBase, PaginationMixin
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FederalAccountProgramActivitiesTotal(PaginationMixin, FederalAccountBase):
    """
    Retrieve a list of program activity totals for a federal account.
    """

    endpoint_doc = (
        "usaspending_api/api_contracts/contracts/v2/federal_accounts/federal_account_totals/program_activities.md"
    )
    filters: dict

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.default_sort_column = "obligations"
        self.sortable_columns = ["obligations", "code", "name", "type"]

    def post(self, request: Request, *args, **kwargs):
        validated_data = self.validate_data(request.data)
        query = self.get_filter_query(validated_data)
        results = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(
                Q(
                    treasury_account__federal_account__federal_account_code=self.federal_account_code,
                    submission__is_final_balances_for_fy=True,
                )
                & query
            )
            .values(
                code=Coalesce(
                    "program_activity_reporting_key__code",
                    "program_activity__program_activity_code",
                    output_field=TextField(),
                )
            )
            .annotate(
                obligations=Sum("obligations_incurred_by_program_object_class_cpe"),
                name=Coalesce(
                    "program_activity_reporting_key__name",
                    "program_activity__program_activity_name",
                    output_field=TextField(),
                ),
                type=Case(
                    When(program_activity_reporting_key__isnull=False, then=Value("PARK")),
                    default=Value("PAC/PAN"),
                    output_field=TextField(),
                ),
            )
            .order_by(*self.pagination.robust_order_by_fields)
            .values("obligations", "code", "name", "type")
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": page_metadata,
            }
        )
