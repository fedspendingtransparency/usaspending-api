from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import covid_def_code_strings, latest_gtas_of_each_year
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from django.db.models.functions import Coalesce

# Limits the amount of results the spending explorer returns
SPENDING_EXPLORER_LIMIT = 500


class OverviewViewSet(APIView):
    """
    This route sends a request to the backend to retrieve spending data information through various types and filters.
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
        raw_values = latest_gtas_of_each_year().values(
            "budget_authority_appropriation_amount_cpe",
            "other_budgetary_resources_amount_cpe",
            "disaster_emergency_fund_code",
        )

        filtered_values = [
            elem for elem in raw_values if elem["disaster_emergency_fund_code"] in covid_def_code_strings()
        ]

        return [
            {
                "def_code": elem["disaster_emergency_fund_code"],
                "amount": elem["budget_authority_appropriation_amount_cpe"]
                + elem["other_budgetary_resources_amount_cpe"],
            }
            for elem in filtered_values
        ]

    def spending(self, funding):
        return {
            "award_obligations": self.award_obligations(),
            "award_outlays": self.award_outlays(),
            "total_obligations": self.total_obligations(funding),
            "total_outlays": self.total_outlays(),
        }

    def award_obligations(self):
        return sum(
            [
                elem["transaction_obligated_amount"]
                for elem in FinancialAccountsByAwards.objects.filter(
                    disaster_emergency_fund__in=covid_def_code_strings()
                ).values("transaction_obligated_amount")
            ]
        )

    def award_outlays(self):
        return sum(
            [
                elem["amount"]
                for elem in FinancialAccountsByAwards.objects.filter(
                    disaster_emergency_fund__in=covid_def_code_strings()
                )
                .annotate(amount=Coalesce("gross_outlay_amount_by_award_cpe", 0))
                .values("amount")
            ]
        )

    def total_obligations(self, funding):
        return (
            sum([elem["amount"] for elem in funding])
            - latest_gtas_of_each_year()
            .filter(fiscal_year=current_fiscal_year())
            .values("unobligated_balance_cpe")
            .first()["unobligated_balance_cpe"]
        )

    def total_outlays(self):
        return sum(
            elem["gross_outlay_amount_by_tas_cpe"]
            for elem in latest_gtas_of_each_year().values("gross_outlay_amount_by_tas_cpe")
        )
