from django.conf.urls import url
from usaspending_api.references.v2.views.autocomplete import AwardingAgencyAutocompleteViewSet, \
    FundingAgencyAutocompleteViewSet, CFDAAutocompleteViewSet, \
    NAICSAutocompleteViewSet, PSCAutocompleteViewSet, RecipientAutocompleteViewSet

urlpatterns = [
    url(r'^awarding_agency', AwardingAgencyAutocompleteViewSet.as_view()),
    url(r'^funding_agency', FundingAgencyAutocompleteViewSet.as_view()),
    url(r'^cfda', CFDAAutocompleteViewSet.as_view()),
    url(r'^naics', NAICSAutocompleteViewSet.as_view()),
    url(r'^psc', PSCAutocompleteViewSet.as_view()),
    url(r'^recipient', RecipientAutocompleteViewSet.as_view()),
    ]
