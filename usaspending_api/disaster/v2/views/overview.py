from rest_framework.response import Response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    covid_def_code_strings,
    latest_gtas_of_each_year_queryset,
    latest_faba_of_each_year_queryset,
)
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from django.db.models.functions import Coalesce
from django.db.models import Sum
from decimal import Decimal


class OverviewViewSet(DisasterBase):
    """
    This route gathers aggregate data about Disaster spending
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/overview.md"

    @cache_response()
    def get(self, request):

        funding = self.funding()
        return Response(
            {
                "funding": funding,
                "total_budget_authority": sum([elem["amount"] for elem in funding]),
                "spending": self.spending(funding),
            }
        )

    def funding(self):
        raw_values = (
            latest_gtas_of_each_year_queryset()
            .values("disaster_emergency_fund_code",)
            .annotate(
                budget_authority_appropriation_amount_cpe=Sum("budget_authority_appropriation_amount_cpe"),
                other_budgetary_resources_amount_cpe=Sum("other_budgetary_resources_amount_cpe"),
            )
        )

        return [
            {
                "def_code": elem["disaster_emergency_fund_code"],
                "amount": elem["budget_authority_appropriation_amount_cpe"]
                + elem["other_budgetary_resources_amount_cpe"],
            }
            for elem in raw_values
        ]

    def spending(self, funding):
        return {
            "award_obligations": self.award_obligations(),
            "award_outlays": self.award_outlays(),
            "total_obligations": self.total_obligations(funding),
            "total_outlays": self.total_outlays(),
        }

    def award_obligations(self):
        return (
            FinancialAccountsByAwards.objects.filter(disaster_emergency_fund__in=covid_def_code_strings())
            .values("transaction_obligated_amount")
            .aggregate(total=Sum("transaction_obligated_amount"))["total"]
            or 0.0
        )

    def award_outlays(self):
        return (
            latest_faba_of_each_year_queryset()
            .annotate(amount=Coalesce("gross_outlay_amount_by_award_cpe", 0))
            .aggregate(total=Sum("amount"))["total"]
        ) or 0.0

    def total_obligations(self, funding):
        remaining_balance = latest_gtas_of_each_year_queryset().order_by("-fiscal_year").first()
        if remaining_balance:
            remaining_balance = remaining_balance.unobligated_balance_cpe
        else:
            remaining_balance = Decimal("0.0")
        return sum([elem["amount"] for elem in funding]) - remaining_balance

    def total_outlays(self):
        return (
            latest_gtas_of_each_year_queryset()
            .values("gross_outlay_amount_by_tas_cpe")
            .aggregate(total=Sum("gross_outlay_amount_by_tas_cpe"))["total"]
            or 0.0
        )
