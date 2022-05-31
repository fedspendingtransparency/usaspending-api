from django.urls import re_path

from usaspending_api.accounts.views import tas as views
from usaspending_api.common.views import RemovedEndpointView

# bind ViewSets to URLs
tas_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_detail = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_balances_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_balances_quarters_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_balances_quarters_total = views.TASBalancesQuarterAggregate.as_view({"get": "list", "post": "list"})
tas_balances_total = views.TASBalancesAggregate.as_view({"get": "list", "post": "list"})
tas_categories_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_categories_total = views.TASCategoryAggregate.as_view({"get": "list", "post": "list"})
tas_categories_quarters_list = RemovedEndpointView.as_view({"get": "retrieve", "post": "retrieve"})
tas_categories_quarters_total = views.TASCategoryQuarterAggregate.as_view({"get": "list", "post": "list"})

urlpatterns = [
    re_path(r"^balances/$", tas_balances_list),
    re_path(r"^balances/total/", tas_balances_total),
    re_path(r"^balances/quarters/$", tas_balances_quarters_list),
    re_path(r"^balances/quarters/total/$", tas_balances_quarters_total),
    re_path(r"^categories/$", tas_categories_list),
    re_path(r"^categories/total/", tas_categories_total),
    re_path(r"^categories/quarters/$", tas_categories_quarters_list),
    re_path(r"^categories/quarters/total/$", tas_categories_quarters_total),
    re_path(r"^$", tas_list),
    re_path(r"(?P<pk>[0-9]+)/$", tas_detail),
    re_path(r"^autocomplete/", views.TreasuryAppropriationAccountAutocomplete.as_view()),
]
