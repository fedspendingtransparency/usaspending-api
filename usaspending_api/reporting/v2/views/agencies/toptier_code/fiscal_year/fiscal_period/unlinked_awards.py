from django.conf import settings
from django.db.models import F
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.reporting.models import ReportingAgencyOverview


class UnlinkedAwards(AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/fiscal_year/fiscal_period/unlinked_awards.md"

    annotation_options = {
        "assistance": {
            "unlinked_file_c_award_count": F("unlinked_assistance_c_awards"),
            "unlinked_file_d_award_count": F("unlinked_assistance_d_awards"),
            "total_linked_award_count": F("linked_assistance_awards"),
        },
        "procurement": {
            "unlinked_file_c_award_count": F("unlinked_procurement_c_awards"),
            "unlinked_file_d_award_count": F("unlinked_procurement_d_awards"),
            "total_linked_award_count": F("linked_procurement_awards"),
        },
    }

    tinyshield_model = [
        {
            "key": "type",
            "name": "type",
            "type": "enum",
            "enum_values": ["assistance", "procurement"],
            "optional": False,
            "default": None,
            "allow_nulls": False,
        },
        {
            "key": "fiscal_year",
            "name": "fiscal_year",
            "type": "integer",
            "min": fy(settings.API_SEARCH_MIN_DATE),
            "max": current_fiscal_year(),
            "optional": False,
            "default": None,
            "allow_nulls": False,
        },
        {
            "key": "fiscal_period",
            "name": "fiscal_period",
            "type": "integer",
            "min": 2,
            "max": 12,
            "optional": False,
            "default": None,
            "allow_nulls": False,
        },
    ]

    @cache_response()
    def get(self, request, toptier_code, fiscal_year, fiscal_period, type):
        my_request = {"type": type, "fiscal_year": fiscal_year, "fiscal_period": fiscal_period}
        validated = TinyShield(self.tinyshield_model).block(my_request)

        self.annotations = self.annotation_options[validated["type"]]
        self.fiscal_year = validated["fiscal_year"]
        self.fiscal_period = validated["fiscal_period"]

        return Response(self.get_unlinked_awards())

    def get_unlinked_awards(self):
        result = (
            ReportingAgencyOverview.objects.filter(
                toptier_code=self.toptier_code, fiscal_year=self.fiscal_year, fiscal_period=self.fiscal_period
            )
            .annotate(**self.annotations)
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
