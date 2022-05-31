from django.urls import re_path
from usaspending_api.references.v2.views.autocomplete import (
    AwardingAgencyAutocompleteViewSet,
    FundingAgencyAutocompleteViewSet,
    CFDAAutocompleteViewSet,
    NAICSAutocompleteViewSet,
    PSCAutocompleteViewSet,
    GlossaryAutocompleteViewSet,
)
from usaspending_api.references.v2.views.city import CityAutocompleteViewSet
from usaspending_api.references.v2.views.tas_autocomplete import (
    TASAutocompleteATA,
    TASAutocompleteAID,
    TASAutocompleteBPOA,
    TASAutocompleteEPOA,
    TASAutocompleteA,
    TASAutocompleteMAIN,
    TASAutocompleteSUB,
)
from usaspending_api.common.views import RemovedEndpointView


urlpatterns = [
    re_path(r"^awarding_agency", AwardingAgencyAutocompleteViewSet.as_view()),
    re_path(r"^funding_agency", FundingAgencyAutocompleteViewSet.as_view()),
    re_path(r"^cfda", CFDAAutocompleteViewSet.as_view()),
    re_path(r"^naics", NAICSAutocompleteViewSet.as_view()),
    re_path(r"^psc", PSCAutocompleteViewSet.as_view()),
    re_path(r"^recipient", RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})),
    re_path(r"^glossary", GlossaryAutocompleteViewSet.as_view()),
    re_path(r"^city", CityAutocompleteViewSet.as_view()),
    re_path(r"^accounts/ata", TASAutocompleteATA.as_view()),
    re_path(r"^accounts/aid", TASAutocompleteAID.as_view()),
    re_path(r"^accounts/bpoa", TASAutocompleteBPOA.as_view()),
    re_path(r"^accounts/epoa", TASAutocompleteEPOA.as_view()),
    re_path(r"^accounts/a", TASAutocompleteA.as_view()),
    re_path(r"^accounts/main", TASAutocompleteMAIN.as_view()),
    re_path(r"^accounts/sub", TASAutocompleteSUB.as_view()),
]
