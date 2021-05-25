from django.db.models import F
from rest_framework.response import Response

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.reporting.models import ReportingAgencyOverview


class UnlinkedAwards(AgencyBase):
    """Returns submission history of the specified agency for the specified fiscal year and period"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/reporting/agencies/toptier_code/fiscal_year/fiscal_period/unlinked_awards/type.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.annotation_options = {
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
        self.additional_models = [
            {
                "key": "type",
                "name": "type",
                "type": "enum",
                "enum_values": list(self.annotation_options),
                "optional": False,
            }
        ]

    @cache_response()
    def get(self, request, toptier_code, fiscal_year, fiscal_period, type):
        validated = self.validated_url_params
        filters = {
            "toptier_code": self.toptier_code,
            "fiscal_year": validated["fiscal_year"],
            "fiscal_period": validated["fiscal_period"],
        }
        annotations = self.annotation_options[validated["type"]]

        return Response(self.get_unlinked_awards(filters, annotations))

    def get_unlinked_awards(self, filters, annotations):
        result = (
            ReportingAgencyOverview.objects.filter(**filters)
            .annotate(**annotations)
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
