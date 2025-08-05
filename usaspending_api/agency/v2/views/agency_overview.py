from django.conf import settings
from django.db.models import Exists, OuterRef
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import SubtierAgency


class AgencyOverview(AgencyBase):
    """
    Returns agency overview information for USAspending.gov's Agency Details page for agencies
    that have ever awarded.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year"]

    @cache_response()
    def get(self, request, *args, **kwargs):
        return Response(
            {
                "fiscal_year": self.fiscal_year,
                "toptier_code": self.toptier_agency.toptier_code,
                "name": self.toptier_agency.name,
                "abbreviation": self.toptier_agency.abbreviation,
                "agency_id": self.agency_id,
                "icon_filename": self.toptier_agency.icon_filename,
                "mission": self.toptier_agency.mission,
                "website": self.toptier_agency.website,
                "congressional_justification_url": self.toptier_agency.justification,
                "about_agency_data": self.toptier_agency.about_agency_data,
                "subtier_agency_count": self.get_subtier_agency_count(),
                "def_codes": self.get_defc(),
                "messages": [],  # Currently no applicable messages
            }
        )

    def get_subtier_agency_count(self):
        return SubtierAgency.objects.filter(
            Exists(
                TransactionNormalized.objects.filter(
                    fiscal_year__gte=fy(settings.API_SEARCH_MIN_DATE),
                    awarding_agency__subtier_agency=OuterRef("subtier_agency_id"),
                )
            ),
            agency__toptier_agency=self.toptier_agency,
        ).count()

    def get_defc(self):
        defc = (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(
                treasury_account__funding_toptier_agency=self.toptier_agency,
                disaster_emergency_fund_id__isnull=False,
                submission__reporting_fiscal_year=self.fiscal_year,
            )
            .values(
                "disaster_emergency_fund",
                "disaster_emergency_fund__group_name",
                "disaster_emergency_fund__public_law",
                "disaster_emergency_fund__title",
                "disaster_emergency_fund__urls",
            )
            .distinct()
        )
        results = [
            {
                "code": x["disaster_emergency_fund"],
                "public_law": x["disaster_emergency_fund__public_law"],
                "title": x["disaster_emergency_fund__title"],
                "urls": x["disaster_emergency_fund__urls"],
                "disaster": x["disaster_emergency_fund__group_name"],
            }
            for x in defc
        ]
        return results
