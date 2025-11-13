from django.db.models import Q, Sum, F
from rest_framework.response import Response
from rest_framework.request import Request

from usaspending_api.accounts.v2.views.federal_account_base import FederalAccountBase
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.pagination_mixin import PaginationMixin
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FederalAccountObjectClassesTotal(PaginationMixin, FederalAccountBase):
    """
    Retrieve a list of program activity totals for a federal account.
    """

    endpoint_doc = (
        "usaspending_api/api_contracts/contracts/v2/federal_accounts/federal_account_totals/object_classes.md"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.default_sort_column = "obligations"
        self.sortable_columns = ["obligations", "code", "name"]

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
            .annotate(code=F("object_class__object_class"))
            .values("code")
            .annotate(
                obligations=Sum("obligations_incurred_by_program_object_class_cpe"),
                name=F("object_class__object_class_name"),
            )
            .order_by(*self.pagination.robust_order_by_fields)
            .values("obligations", "code", "name")
        )
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": page_metadata,
            }
        )
