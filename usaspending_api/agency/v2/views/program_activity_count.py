from django.db.models import Exists, OuterRef
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import RefProgramActivity


class ProgramActivityCount(AgencyBase):
    """
    Obtain the count of program activity categories for a specific agency in a
    single fiscal year based on whether or not that program activity has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/program_activity/count.md"

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "program_activity_count": self.get_program_activity_count(),
                "messages": self.standard_response_messages,
            }
        )

    def get_program_activity_count(self):
        return (
            RefProgramActivity.objects.annotate(
                include=Exists(
                    FinancialAccountsByProgramActivityObjectClass.objects.filter(
                        program_activity_id=OuterRef("pk"),
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
