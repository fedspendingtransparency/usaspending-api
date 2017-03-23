from django.conf.urls import url

from usaspending_api.accounts import views

# bind ViewSets to URLs
tas_list = views.TreasuryAppropriationAccountViewSet.as_view(
    {'get': 'list', 'post': 'list'})
tas_balances_list = views.TreasuryAppropriationAccountBalancesViewSet.as_view(
    {'get': 'list', 'post': 'list'})
financial_accounts_by_award = views.FinancialAccountsByAwardListViewSet.as_view(
    {'get': 'list', 'post': 'list'})
tas_balances_total = views.TASBalancesAggregate.as_view({
    'get': 'list', 'post': 'list'})
tas_categories_list = views.TASCategoryList.as_view(
    {'get': 'list', 'post': 'list'})
tas_categories_total = views.TASCategoryAggregate.as_view({
    'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^tas/balances/$', tas_balances_list),
    url(r'^tas/balances/total/', tas_balances_total),
    url(r'^tas/categories/$', tas_categories_list),
    url(r'^tas/categories/total/', tas_categories_total),
    url(r'^tas/$', tas_list),
    url(r'^awards/', financial_accounts_by_award),
    url(r'^tas/autocomplete/', views.TreasuryAppropriationAccountAutocomplete.as_view())
]
