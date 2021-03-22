from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response

# from django.db.models import Exists, OuterRef, Q
# from typing import Any
# from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
# from usaspending_api.references.models import ObjectClass


class ObligationsByCategory(AgencyBase):
    """
    Returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other) within the requested fiscal year (or current FY).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/obligations_by_type.md"

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(
            {
                # "total_aggregated_amount": self.total_amount,
                "results": self.get_category_amounts(),
                "messages": self.standard_response_messages,
            }
        )

    def get_category_amounts(self):
        return (
            TransactionNormalized.objects.filter(federal_action_obligation__isnull=False, fiscal_year=self.fiscal_year)
            # .annotate(total_aggregated_amount=Sum("federal_action_obligation"))
            .annotate(
                award_category=F("award__category"),
                aggregated_amount=Sum("federal_action_obligation"),
            )
            .order_by("-total_aggregated_amount")
        )
