from decimal import Decimal
from django.db.models import Sum, F
from rest_framework.response import Response

from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    filter_by_defc_closed_periods,
    latest_faba_of_each_year_queryset,
    latest_gtas_of_each_year_queryset,
)
from usaspending_api.references.models import DisasterEmergencyFundCode


class OverviewViewSet(DisasterBase):
    """
    This route gathers aggregate data about Disaster spending
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/overview.md"

    @cache_response()
    def get(self, request):

        request_values = self._parse_and_validate(request.GET)
        self.defc = request_values["def_codes"].split(",")
        funding, self.total_budget_authority = self.funding()

        return Response(
            {"funding": funding, "total_budget_authority": self.total_budget_authority, "spending": self.spending()}
        )

    def _parse_and_validate(self, request):
        all_def_codes = sorted(list(DisasterEmergencyFundCode.objects.values_list("code", flat=True)))
        models = [
            {
                "key": "def_codes",
                "name": "def_codes",
                "type": "text",
                "text_type": "search",
                "allow_nulls": True,
                "optional": True,
                "default": ",".join(all_def_codes),
            },
        ]
        return TinyShield(models).block(request)

    def funding(self):
        funding = list(
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=self.defc)
            .values("disaster_emergency_fund_code")
            .annotate(
                def_code=F("disaster_emergency_fund_code"),
                amount=Sum("total_budgetary_resources_cpe"),
                unobligated_balance=Sum("budget_authority_unobligated_balance_brought_forward_cpe"),
            )
            .values("def_code", "amount", "unobligated_balance")
        )

        total_budget_authority = self.sum_values(funding, "amount") - self.sum_values(funding, "unobligated_balance")

        for entry in funding:
            del entry["unobligated_balance"]

        return funding, total_budget_authority

    def spending(self):
        remaining_balances = self.remaining_balances()
        award_obligations = self.award_obligations()

        return {
            "award_obligations": award_obligations,
            "award_outlays": self.award_outlays(),
            "total_obligations": self.total_budget_authority - Decimal(remaining_balances),
            "total_outlays": self.total_outlays(),
        }

    def award_obligations(self):
        return (
            FinancialAccountsByAwards.objects.filter(
                filter_by_defc_closed_periods(), disaster_emergency_fund__in=self.defc
            )
            .values("transaction_obligated_amount")
            .aggregate(total=Sum("transaction_obligated_amount"))["total"]
            or 0.0
        )

    def award_outlays(self):
        return (
            latest_faba_of_each_year_queryset()
            .filter(disaster_emergency_fund__in=self.defc)
            .aggregate(total=Sum("gross_outlay_amount_by_award_cpe"))["total"]
        ) or 0.0

    def remaining_balances(self):
        remaining_balances = list(
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=self.defc)
            .values("fiscal_year")
            .annotate(total=Sum("unobligated_balance_cpe"))
            .values("fiscal_year", "total")
            .order_by("-fiscal_year")
        )

        return remaining_balances[0]["total"] if remaining_balances else Decimal("0.0")

    def total_outlays(self):
        return (
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=self.defc)
            .values("gross_outlay_amount_by_tas_cpe")
            .aggregate(total=Sum("gross_outlay_amount_by_tas_cpe"))["total"]
            or 0.0
        )

    def sum_values(self, obj, key):
        return Decimal(sum([elem[key] for elem in obj]))
