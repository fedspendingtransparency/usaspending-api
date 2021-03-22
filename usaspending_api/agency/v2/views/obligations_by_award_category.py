from django.db.models import F, Sum
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import Award
# from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.references.models.agency import Agency

# from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
# from usaspending_api.references.models import ObjectClass


class ObligationsByAwardCategory(AgencyBase):
    """
    Returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other) within the requested fiscal year (or current FY).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/obligations_by_award_category.md"

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        agency_ids = Agency.objects.filter(toptier_agency__toptier_code=kwargs["toptier_code"]).values("id")
        return Response(
            {
                # "total_aggregated_amount": self.total_obligation,
                "results": self.get_category_amounts(agency_ids),
                "messages": self.standard_response_messages,
            }
        )

    def get_category_amounts(self, agency_ids):
        return (
            Award.objects.filter(
                awarding_agency_id__in=agency_ids, fiscal_year=self.fiscal_year
            )
            # .annotate(total_aggregated_amount=Sum("total_obligation"))
            .annotate(
                award_category=F("category"),
                aggregated_amount=Sum("total_obligation"),
            )
            .order_by("-aggregated_amount")
            .values("award_category", "aggregated_amount")
        )
