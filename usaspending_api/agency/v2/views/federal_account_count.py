from django.db.models import Exists, OuterRef, Q
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.accounts.models import FederalAccount, TreasuryAppropriationAccount
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FederalAccountCount(AgencyBase):
    """
    Obtain the count of federal accounts and treasury accounts for a specific agency in a
    single fiscal year
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/federal_account/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "federal_account_count": self.get_federal_account_count(),
                "treasury_account_count": self.get_treasury_account_count(),
                "messages": self.standard_response_messages,
            }
        )

    def get_federal_account_count(self):
        filters = [
            Q(treasury_account__federal_account_id=OuterRef("pk")),
            Q(final_of_fy=True),
            Q(treasury_account__funding_toptier_agency=self.toptier_agency),
            Q(submission__reporting_fiscal_year=self.fiscal_year),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
        ]
        return (
            FederalAccount.objects.annotate(
                include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
            )
            .filter(include=True)
            .values("pk")
            .count()
        )

    def get_treasury_account_count(self):
        return (
            TreasuryAppropriationAccount.objects.annotate(
                include=Exists(
                    FinancialAccountsByProgramActivityObjectClass.objects.filter(
                        treasury_account_id=OuterRef("pk"),
                        final_of_fy=True,
                        treasury_account__funding_toptier_agency=self.toptier_agency,
                        submission__reporting_fiscal_year=self.fiscal_year,
                    ).values("pk")
                )
            )
            .filter(include=True)
            .values("pk")
            .count()
        )
