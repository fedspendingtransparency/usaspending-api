from django.conf.urls import url
from usaspending_api.references.views_v2.autocomplete import RecipientAutocompleteViewSet


urlpatterns = [
    url(r'^recipient/', RecipientAutocompleteViewSet.as_view())
]
