from django.conf import settings
from rest_framework.response import Response
from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.references.models import Agency, SubtierAgency


class AgencyOverview(AgencyBase):
    """
    Returns agency overview information for USAspending.gov's Agency Details page for agencies
    that have ever awarded.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code.md"

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
                "messages": [],  # Currently no applicable messages
            }
        )

    def get_subtier_agency_count(self):
        filters = {"fiscal_year__gte": fy(settings.API_SEARCH_MIN_DATE)}
        values = ["pk", "awarding_agency__subtier_agency"]

        return (
            SubtierAgency.objects.filter(agency__toptier_agency=self.toptier_agency)
            .extra(
                where=[
                    f"Exists({generate_raw_quoted_query(TransactionNormalized.objects.filter(**filters).values(*values))}"
                    f" AND {Agency._meta.db_table}.subtier_agency_id = {SubtierAgency._meta.db_table}.subtier_agency_id)"
                ]
            )
            .count()
        )
