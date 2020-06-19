from django.contrib.postgres.fields import ArrayField
from django.db.models import Q, Sum, Count, F, Value, DecimalField, TextField
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, PaginationMixin, LoansMixin
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class Spending(PaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        # TODO ASAP! Only include Account data from "closed periods"
        filters = [
            Q(final_of_fy=True),
            Q(submission__reporting_fiscal_year__gte=2020),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
        ]

        queryset = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(
                obligation=Sum("obligations_incurred_by_program_object_class_cpe"),
                outlay=Sum("gross_outlay_amount_by_program_object_class_cpe"),
                count=Count("*"),
                id=F("treasury_account__federal_account__id"),
                code=F("treasury_account__federal_account__federal_account_code"),
                description=F("treasury_account__federal_account__account_title"),
                total_budgetary_resources=Value(None, DecimalField(max_digits=23, decimal_places=2)),  # Temporary: GTAS
                children=Value(None, ArrayField(TextField())),
            )
            .values(
                "obligation", "outlay", "count", "id", "code", "description", "total_budgetary_resources", "children"
            )
        )

        results = list(queryset)
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        return Response(
            {
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": page_metadata,
            }
        )


class Loans(LoansMixin, PaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        return Response({"endpoint": self.endpoint_doc})
