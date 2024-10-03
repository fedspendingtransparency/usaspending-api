from django.urls import re_path

from usaspending_api.references.v2.views.autocomplete import (
    AwardingAgencyAutocompleteViewSet,
    AwardingAgencyOfficeAutocompleteViewSet,
    CFDAAutocompleteViewSet,
    FundingAgencyAutocompleteViewSet,
    FundingAgencyOfficeAutocompleteViewSet,
    GlossaryAutocompleteViewSet,
    NAICSAutocompleteViewSet,
    PSCAutocompleteViewSet,
    ProgramActivityAutocompleteViewSet,
)
from usaspending_api.references.v2.views.city import CityAutocompleteViewSet
from usaspending_api.references.v2.views.location_autocomplete import LocationAutocompleteViewSet
from usaspending_api.references.v2.views.recipients import RecipientAutocompleteViewSet
from usaspending_api.references.v2.views.tas_autocomplete import (
    TASAutocompleteA,
    TASAutocompleteAID,
    TASAutocompleteATA,
    TASAutocompleteBPOA,
    TASAutocompleteEPOA,
    TASAutocompleteMAIN,
    TASAutocompleteSUB,
)

urlpatterns = [
    re_path(r"^awarding_agency/$", AwardingAgencyAutocompleteViewSet.as_view()),
    re_path(r"^funding_agency/$", FundingAgencyAutocompleteViewSet.as_view()),
    re_path(r"^awarding_agency_office/$", AwardingAgencyOfficeAutocompleteViewSet.as_view()),
    re_path(r"^funding_agency_office/$", FundingAgencyOfficeAutocompleteViewSet.as_view()),
    re_path(r"^cfda", CFDAAutocompleteViewSet.as_view()),
    re_path(r"^naics", NAICSAutocompleteViewSet.as_view()),
    re_path(r"^psc", PSCAutocompleteViewSet.as_view()),
    re_path(r"^program_activity", ProgramActivityAutocompleteViewSet.as_view()),
    re_path(r"^recipient", RecipientAutocompleteViewSet.as_view()),
    re_path(r"^glossary", GlossaryAutocompleteViewSet.as_view()),
    re_path(r"^city", CityAutocompleteViewSet.as_view()),
    re_path(r"^accounts/ata", TASAutocompleteATA.as_view()),
    re_path(r"^accounts/aid", TASAutocompleteAID.as_view()),
    re_path(r"^accounts/bpoa", TASAutocompleteBPOA.as_view()),
    re_path(r"^accounts/epoa", TASAutocompleteEPOA.as_view()),
    re_path(r"^accounts/a", TASAutocompleteA.as_view()),
    re_path(r"^accounts/main", TASAutocompleteMAIN.as_view()),
    re_path(r"^accounts/sub", TASAutocompleteSUB.as_view()),
    re_path(r"^location", LocationAutocompleteViewSet().as_view()),
]
