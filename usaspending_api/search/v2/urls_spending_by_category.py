from django.urls import re_path

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
    DEFCViewSet,
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
    re_path(r"^awarding_agency", AwardingAgencyViewSet.as_view()),
    re_path(r"^awarding_subagency", AwardingSubagencyViewSet.as_view()),
    re_path(r"^cfda", CfdaViewSet.as_view()),
    re_path(r"^country", CountryViewSet.as_view()),
    re_path(r"^county", CountyViewSet.as_view()),
    re_path(r"^district", DistrictViewSet.as_view()),
    re_path(r"^federal_account", FederalAccountViewSet.as_view()),
    re_path(r"^funding_agency", FundingAgencyViewSet.as_view()),
    re_path(r"^funding_subagency", FundingSubagencyViewSet.as_view()),
    re_path(r"^naics", NAICSViewSet.as_view()),
    re_path(r"^psc", PSCViewSet.as_view()),
    re_path(r"^recipient/?$", RecipientViewSet.as_view()),
    re_path(r"^recipient_duns", RecipientDunsViewSet.as_view()),
    re_path(r"^state_territory", StateTerritoryViewSet.as_view()),
    re_path(r"^defc", DEFCViewSet.as_view()),
]
