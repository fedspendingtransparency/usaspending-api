from django.conf.urls import url
from usaspending_api.references.v2.views.autocomplete import (
    AwardingAgencyAutocompleteViewSet,
    FundingAgencyAutocompleteViewSet,
    CFDAAutocompleteViewSet,
    NAICSAutocompleteViewSet,
    PSCAutocompleteViewSet,
    RecipientAutocompleteViewSet,
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
    TASAutocompleteSUB
)


urlpatterns = [
    url(r"^awarding_agency", AwardingAgencyAutocompleteViewSet.as_view()),
    url(r"^funding_agency", FundingAgencyAutocompleteViewSet.as_view()),
    url(r"^cfda", CFDAAutocompleteViewSet.as_view()),
    url(r"^naics", NAICSAutocompleteViewSet.as_view()),
    url(r"^psc", PSCAutocompleteViewSet.as_view()),
    url(r"^recipient", RecipientAutocompleteViewSet.as_view()),
    url(r"^glossary", GlossaryAutocompleteViewSet.as_view()),
    url(r"^city", CityAutocompleteViewSet.as_view()),
    url(r"^accounts/ata", TASAutocompleteATA.as_view()),
    url(r"^accounts/aid", TASAutocompleteAID.as_view()),
    url(r"^accounts/bpoa", TASAutocompleteBPOA.as_view()),
    url(r"^accounts/epoa", TASAutocompleteEPOA.as_view()),
    url(r"^accounts/a", TASAutocompleteA.as_view()),
    url(r"^accounts/main", TASAutocompleteMAIN.as_view()),
    url(r"^accounts/sub", TASAutocompleteSUB.as_view()),
]
