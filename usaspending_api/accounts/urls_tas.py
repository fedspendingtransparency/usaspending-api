from django.conf.urls import url

from usaspending_api.accounts import views

# bind ViewSets to URLs
tas_list = views.TreasuryAppropriationAccountViewSet.as_view(
    {'get': 'list', 'post': 'list'})
tas_detail = views.TreasuryAppropriationAccountViewSet.as_view(
    {'get': 'retrieve', 'post': 'retrieve'})
tas_balances_list = views.TreasuryAppropriationAccountBalancesViewSet.as_view(
    {'get': 'list', 'post': 'list'})
tas_balances_total = views.TASBalancesAggregate.as_view({
    'get': 'list', 'post': 'list'})
tas_categories_list = views.TASCategoryList.as_view(
    {'get': 'list', 'post': 'list'})
tas_categories_total = views.TASCategoryAggregate.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^balances/$', tas_balances_list),
    url(r'^balances/total/', tas_balances_total),
    url(r'^categories/$', tas_categories_list),
    url(r'^categories/total/', tas_categories_total),
    url(r'^$', tas_list),
    url(r'(?P<pk>[0-9]+)/$', tas_detail),
    url(r'^autocomplete/', views.TreasuryAppropriationAccountAutocomplete.as_view())
]
