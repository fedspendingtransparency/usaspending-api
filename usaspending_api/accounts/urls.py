from django.conf.urls import url

from usaspending_api.accounts.views import common as views

# bind ViewSets to URLs
financial_accounts_by_award = views.FinancialAccountsByAwardListViewSet.as_view(
    {'get': 'list', 'post': 'list'})

urlpatterns = [
    url(r'^awards/', financial_accounts_by_award),
]
