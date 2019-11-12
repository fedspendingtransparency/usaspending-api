from django.conf.urls import url

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
    url(r"^balances/$", tas_balances_list),
    url(r"^balances/total/", tas_balances_total),
    url(r"^balances/quarters/$", tas_balances_quarters_list),
    url(r"^balances/quarters/total/$", tas_balances_quarters_total),
    url(r"^categories/$", tas_categories_list),
    url(r"^categories/total/", tas_categories_total),
    url(r"^categories/quarters/$", tas_categories_quarters_list),
    url(r"^categories/quarters/total/$", tas_categories_quarters_total),
    url(r"^$", tas_list),
    url(r"(?P<pk>[0-9]+)/$", tas_detail),
    url(r"^autocomplete/", views.TreasuryAppropriationAccountAutocomplete.as_view()),
]
