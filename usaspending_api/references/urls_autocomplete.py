from django.conf.urls import url
from usaspending_api.references.views_v2.autocomplete import AwardingAgencyAutocompleteViewSet, \
    BudgetFunctionAutocompleteViewSet,CFDAAutocompleteViewSet, FundingAgencyAutocompleteViewSet, \
    NAICSAutocompleteViewSet, PSCAutocompleteViewSet, RecipientAutocompleteViewSet


urlpatterns = [
    url(r'^awarding_agency', AwardingAgencyAutocompleteViewSet.as_view()),
    url(r'^budget_function', BudgetFunctionAutocompleteViewSet.as_view()),
    url(r'^cfda', CFDAAutocompleteViewSet.as_view()),
    url(r'^funding_agency', FundingAgencyAutocompleteViewSet.as_view()),
    url(r'^naics', NAICSAutocompleteViewSet.as_view()),
    url(r'^psc', PSCAutocompleteViewSet.as_view()),
    url(r'^recipient/', RecipientAutocompleteViewSet.as_view())
]
