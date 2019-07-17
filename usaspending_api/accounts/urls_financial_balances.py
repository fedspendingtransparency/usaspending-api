from django.conf.urls import url

from usaspending_api.accounts.views import financial_balances as views

# bind ViewSets to URLs
financial_balances_agencies = views.AgenciesFinancialBalancesViewSet.as_view({"get": "list"})

urlpatterns = [url(r"^agencies/$", financial_balances_agencies)]
