from django.urls import re_path

from usaspending_api.accounts.views import federal_account as views

# bind ViewSets to URLs
federal_list = views.FederalAccountViewSet.as_view({"get": "list", "post": "list"})
federal_detail = views.FederalAccountViewSet.as_view({"get": "retrieve", "post": "retrieve"})
federal_autocomplete = views.FederalAccountAutocomplete.as_view()

urlpatterns = [
    re_path(r"^$", federal_list),
    re_path(r"(?P<pk>[0-9]+)/$", federal_detail),
    re_path(r"^autocomplete/", federal_autocomplete),
]
