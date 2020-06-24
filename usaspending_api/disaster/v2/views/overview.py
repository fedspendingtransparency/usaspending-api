from rest_framework.response import Response
from rest_framework.views import APIView
from django.db.models import Exists, OuterRef, Max
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.disaster.v2.views.disaster_base import covid_def_codes

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
                "spending": self.spending(),
            }
        )

    def funding(self):
        raw_values = (
            GTASSF133Balances.objects.annotate(
                include=Exists(
                    GTASSF133Balances.objects.values("fiscal_year")
                    .annotate(fiscal_period_max=Max("fiscal_period"))
                    .values("fiscal_year", "fiscal_period_max")
                    .filter(
                        fiscal_year=OuterRef("fiscal_year"),
                        fiscal_year__gte=2020,
                        fiscal_period_max=OuterRef("fiscal_period"),
                    )
                )
            )
            .filter(include=True)
            .values(
                "budget_authority_appropriation_amount_cpe",
                "other_budgetary_resources_amount_cpe",
                "disaster_emergency_fund_code",
            )
        )

        filtered_values = [
            elem
            for elem in raw_values
            if elem["disaster_emergency_fund_code"] in [code["code"] for code in covid_def_codes().values("code")]
        ]

        return [
            {
                "def_code": elem["disaster_emergency_fund_code"],
                "amount": elem["budget_authority_appropriation_amount_cpe"]
                + elem["other_budgetary_resources_amount_cpe"],
            }
            for elem in filtered_values
        ]

    def spending(self):
        return {
            "award_obligations": 866700000000,
            "award_outlays": 413100000000,
            "total_obligations": 963000000000,
            "total_outlays": 459000000000,
        }
