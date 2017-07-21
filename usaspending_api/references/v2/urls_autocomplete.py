from django.conf.urls import url

from usaspending_api.references.v2.views.autocomplete import AwardingAgencyAutocompleteViewSet, \
    BudgetFunctionAutocompleteViewSet, FundingAgencyAutocompleteViewSet, RecipientAutocompleteViewSet, \
    ToptierAgencyAutocompleteViewSet

urlpatterns = [
    url(r'^awarding_agency', AwardingAgencyAutocompleteViewSet.as_view()),
    url(r'^budget_function', BudgetFunctionAutocompleteViewSet.as_view()),
    url(r'^funding_agency', FundingAgencyAutocompleteViewSet.as_view()),
    url(r'^recipient', RecipientAutocompleteViewSet.as_view()),
    url(r'^toptier_agency', ToptierAgencyAutocompleteViewSet.as_view())
]
