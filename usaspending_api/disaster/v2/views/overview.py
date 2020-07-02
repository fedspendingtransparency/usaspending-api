from rest_framework.response import Response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    latest_gtas_of_each_year_queryset,
    latest_faba_of_each_year_queryset,
)
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from django.db.models.functions import Coalesce
from django.db.models import Sum
from usaspending_api.common.validator.tinyshield import TinyShield
from decimal import Decimal


class OverviewViewSet(DisasterBase):
    """
    This route gathers aggregate data about Disaster spending
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/overview.md"

    @cache_response()
    def get(self, request):

        request_values = self._parse_and_validate(request.GET)
        defc = request_values["def_codes"].split(",")

        funding = self.funding(defc)
        return Response(
            {
                "funding": funding,
                "total_budget_authority": sum([elem["amount"] for elem in funding]),
                "spending": self.spending(funding, defc),
            }
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

    def funding(self, defc):
        raw_values = (
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=defc)
            .values("disaster_emergency_fund_code")
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

    def spending(self, funding, defc):
        return {
            "award_obligations": self.award_obligations(defc),
            "award_outlays": self.award_outlays(defc),
            "total_obligations": self.total_obligations(funding, defc),
            "total_outlays": self.total_outlays(defc),
        }

    def award_obligations(self, defc):
        return (
            FinancialAccountsByAwards.objects.filter(disaster_emergency_fund__in=defc)
            .values("transaction_obligated_amount")
            .aggregate(total=Sum("transaction_obligated_amount"))["total"]
            or 0.0
        )

    def award_outlays(self, defc):
        return (
            latest_faba_of_each_year_queryset()
            .filter(disaster_emergency_fund__in=defc)
            .annotate(amount=Coalesce("gross_outlay_amount_by_award_cpe", 0))
            .aggregate(total=Sum("amount"))["total"]
        ) or 0.0

    def total_obligations(self, funding, defc):
        remaining_balance = (
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=defc)
            .order_by("-fiscal_year")
            .first()
        )
        if remaining_balance:
            remaining_balance = remaining_balance.unobligated_balance_cpe
        else:
            remaining_balance = Decimal("0.0")
        return sum([elem["amount"] for elem in funding]) - remaining_balance

    def total_outlays(self, defc):
        return (
            latest_gtas_of_each_year_queryset()
            .filter(disaster_emergency_fund_code__in=defc)
            .values("gross_outlay_amount_by_tas_cpe")
            .aggregate(total=Sum("gross_outlay_amount_by_tas_cpe"))["total"]
            or 0.0
        )
