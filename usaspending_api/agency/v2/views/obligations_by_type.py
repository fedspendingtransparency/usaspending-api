from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response

# from django.db.models import Exists, OuterRef, Q
# from typing import Any
# from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
# from usaspending_api.references.models import ObjectClass


class ObligationsByType(AgencyBase):
    """
    Returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other) within the requested fiscal year (or current FY).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/obligations_by_type.md"

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(
            {
                "total_aggregated_amount": self.total_amount,
                "results": self.get_category_amounts(),
                "messages": self.standard_response_messages,
            }
        )

    def get_category_amounts(self):
        queryset = (
            TransactionNormalized.objects.filter(federal_action_obligation__isnull=False, fiscal_year=self.fiscal_year)
            .annotate(
                award_category=F("award__category"),
                total_aggregated_amount=Sum("federal_action_obligation"),
                recipient_name=Coalesce(
                    F("award__latest_transaction__assistance_data__awardee_or_recipient_legal"),
                    F("award__latest_transaction__contract_data__awardee_or_recipient_legal"),
                ),
            )
            .order_by("-total_aggregated_amount")
        )







        filters = [
            Q(object_class_id=OuterRef("pk")),
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
            ObjectClass.objects.annotate(
                include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
            )
            .filter(include=True)
            .values("pk")
            .count()
        )
