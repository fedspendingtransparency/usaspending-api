from django.conf.urls import url
from usaspending_api.references.views_v2.autocomplete import BudgetFunctionAutocompleteViewSet


urlpatterns = [
    url(r'^budget_function/', BudgetFunctionAutocompleteViewSet.as_view())
]
