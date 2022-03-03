from django.conf.urls import url

from usaspending_api.search.v2.views.spending_by_category_views.spending_by_agency_types import (
    AwardingAgencyViewSet,
    AwardingSubagencyViewSet,
    FundingAgencyViewSet,
    FundingSubagencyViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_industry_codes import (
    CfdaViewSet,
    PSCViewSet,
    NAICSViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_federal_account import FederalAccountViewSet
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_locations import (
    CountryViewSet,
    CountyViewSet,
    DistrictViewSet,
    StateTerritoryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient import (
    RecipientViewSet,
    RecipientDunsViewSet,
)

urlpatterns = [
    url(r"^awarding_agency", AwardingAgencyViewSet.as_view()),
    url(r"^awarding_subagency", AwardingSubagencyViewSet.as_view()),
    url(r"^cfda", CfdaViewSet.as_view()),
    url(r"^country", CountryViewSet.as_view()),
    url(r"^county", CountyViewSet.as_view()),
    url(r"^district", DistrictViewSet.as_view()),
    url(r"^federal_account", FederalAccountViewSet.as_view()),
    url(r"^funding_agency", FundingAgencyViewSet.as_view()),
    url(r"^funding_subagency", FundingSubagencyViewSet.as_view()),
    url(r"^naics", NAICSViewSet.as_view()),
    url(r"^psc", PSCViewSet.as_view()),
    url(r"^recipient$", RecipientViewSet.as_view()),
    url(r"^recipient_duns", RecipientDunsViewSet.as_view()),
    url(r"^state_territory", StateTerritoryViewSet.as_view()),
]
