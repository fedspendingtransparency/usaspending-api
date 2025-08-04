from django.db.models import Exists, OuterRef, Q
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import RefProgramActivity
from usaspending_api.submissions.helpers import get_latest_submission_ids_for_fiscal_year


class ProgramActivityCount(AgencyBase):
    """
    Obtain the count of program activity categories for a specific agency in a
    single fiscal year based on whether or not that program activity has ever
    been submitted in File B.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/program_activity/count.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year"]

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
        submission_ids = get_latest_submission_ids_for_fiscal_year(self.fiscal_year)
        file_b_calculations = FileBCalculations()
        filters = [
            Q(program_activity_id=OuterRef("pk")),
            Q(submission_id__in=submission_ids),
            Q(treasury_account__funding_toptier_agency=self.toptier_agency),
            file_b_calculations.is_non_zero_total_spending(),
        ]
        return (
            RefProgramActivity.objects.annotate(
                include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
            )
            .filter(include=True)
            .values("pk")
            .count()
        )
