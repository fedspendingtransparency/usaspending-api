from django.db.models import F
from rest_framework import status
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.reporting.models import ReportingAgencyOverview


class UnlinkedAwards(AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/fiscal_year/fiscal_period/unlinked_awards.md"

    assistance_annotations = {
        "unlinked_file_c_award_count": F("unlinked_assistance_c_awards"),
        "unlinked_file_d_award_count": F("unlinked_assistance_d_awards"),
        "total_linked_award_count": F("linked_assistance_awards"),
    }

    procurement_annotations = {
        "unlinked_file_c_award_count": F("unlinked_procurement_c_awards"),
        "unlinked_file_d_award_count": F("unlinked_procurement_d_awards"),
        "total_linked_award_count": F("linked_procurement_awards"),
    }

    def get(self, request, toptier_code, fiscal_year, fiscal_period, type):
        # This private attribute is used by AgencyBase to validate year
        self._fiscal_year = fiscal_year

        self.validate_fiscal_period({"fiscal_period": int(fiscal_period)})
        self.fiscal_period = int(fiscal_period)

        if type == "assistance":
            self.type_annotations = self.assistance_annotations
        elif type == "procurement":
            self.type_annotations = self.procurement_annotations
        else:
            raise UnprocessableEntityException("Type must be either 'assistance' or 'procurement'")

        result = self.get_unlinked_awards()

        return Response(result)

    def get_unlinked_awards(self):
        result = (
            ReportingAgencyOverview.objects.filter(
                toptier_code=self.toptier_code, fiscal_year=self.fiscal_year, fiscal_period=self.fiscal_period
            )
            .annotate(**self.type_annotations)
            .values(
                "unlinked_file_c_award_count",
                "unlinked_file_d_award_count",
                "total_linked_award_count",
            )
            .first()
        )

        if not result:
            result = {
                "unlinked_file_c_award_count": 0,
                "unlinked_file_d_award_count": 0,
                "total_linked_award_count": 0,
            }

        return result
