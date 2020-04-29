from django.conf import settings
from django.db.models import Exists, OuterRef
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.references.models import SubtierAgency


class AgencyOverview(AgencyBase):
    """
    Returns agency overview information for USAspending.gov's Agency Details page for agencies
    that have ever awarded.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code.md"

    @cache_response()
    def get(self, request, *args, **kwargs):
        toptier_agency = self.toptier_agency
        fiscal_year = self.fiscal_year
        min_fiscal_year = fy(settings.API_SEARCH_MIN_DATE)
        subtier_agency_count = (
            SubtierAgency.objects.filter(agency__toptier_agency=self.toptier_agency)
            .annotate(
                was_an_awarding_agency=Exists(
                    TransactionNormalized.objects.filter(
                        fiscal_year__gte=min_fiscal_year, awarding_agency__subtier_agency=OuterRef("pk")
                    ).values("pk")
                )
            )
            .filter(was_an_awarding_agency=True)
            .values("pk")
            .count()
        )

        return Response(
            {
                "fiscal_year": fiscal_year,
                "toptier_code": toptier_agency.toptier_code,
                "name": toptier_agency.name,
                "abbreviation": toptier_agency.abbreviation,
                "icon_filename": toptier_agency.icon_filename,
                "mission": toptier_agency.mission,
                "website": toptier_agency.website,
                "congressional_justification_url": toptier_agency.justification,
                "about_agency_data": toptier_agency.about_agency_data,
                "subtier_agency_count": subtier_agency_count,
            }
        )
