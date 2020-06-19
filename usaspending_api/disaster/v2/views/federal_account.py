from django.db.models import Q, Sum, Count, F, Value, DecimalField
from django.db.models.functions import Concat
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.data_classes import FedAcctResults, FederalAccount, TreasuryAccount
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, PaginationMixin, LoansMixin, SpendingMixin
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class Spending(PaginationMixin, SpendingMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            from usaspending_api.common.exceptions import NotImplementedException
            raise NotImplementedException("ToDo")

        results = list(self.get_queryset())
        federal_accounts = FedAcctResults()
        for row in results:
            FA = FederalAccount(id=row["fa_id"], code=row["fa_code"], description=row["fa_description"],)
            TAS = TreasuryAccount(
                id=row["id"],
                code=row["code"],
                count=row["count"],
                description=row["description"],
                obligation=row["obligation"],
                outlay=row["outlay"],
                total_budgetary_resources=row["total_budgetary_resources"],
            )

            federal_accounts.add_if_missing(FA)
            federal_accounts[FA].include(TAS)

        page_metadata = get_pagination_metadata(len(federal_accounts), self.pagination.limit, self.pagination.page)

        return Response(
            {
                "results": federal_accounts.finalize(
                    self.pagination.sort_key,
                    self.pagination.sort_order,
                    self.pagination.lower_limit,
                    self.pagination.upper_limit,
                ),
                "page_metadata": page_metadata,
            }
        )

    def get_queryset(self):
        filters = [
            Q(final_of_fy=True),  # TODO ASAP! Only include Account data from "closed periods" using lookup table
            Q(
                Q(Q(submission__reporting_fiscal_year=2020) & Q(submission__reporting_fiscal_period__gte=1))
                | Q(submission__reporting_fiscal_year__gte=2021)
            ),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
        ]
        annotations = {
            "fa_code": Concat(
                F("treasury_account__main_account_code"), Value("-"), F("treasury_account__sub_account_code")
            ),
            "count": Count("*"),
            "description": F("treasury_account__tas_rendering_label"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Sum("obligations_incurred_by_program_object_class_cpe"),
            "outlay": Sum("gross_outlay_amount_by_program_object_class_cpe"),
            "total_budgetary_resources": Value(None, DecimalField(max_digits=23, decimal_places=2)),  # Temporary: GTAS
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )


class Loans(LoansMixin, PaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        return Response({"endpoint": self.endpoint_doc})
