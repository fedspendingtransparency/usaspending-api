from django.conf.urls import url

from usaspending_api.accounts import views

# bind ViewSets to URLs
tas_list = views.TreasuryAppropriationAccountViewSet.as_view(
    {'get': 'list', 'post': 'list'})
tas_balances_list = views.TreasuryAppropriationAccountBalancesViewSet.as_view(
    {'get': 'list', 'post': 'list'})
urlpatterns = [
    url(r'^$', tas_balances_list),
    url(r'^tas/', tas_list)
]
