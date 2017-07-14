from django.conf.urls import url
from usaspending_api.references.views_v2.autocomplete import BudgetFunctionAutocompleteViewSet, \
    RecipientAutocompleteViewSet


urlpatterns = [
    url(r'^budget_function', BudgetFunctionAutocompleteViewSet.as_view()),
    url(r'^recipient/', RecipientAutocompleteViewSet.as_view())
]
