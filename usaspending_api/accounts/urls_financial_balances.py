from django.urls import re_path

from usaspending_api.accounts.views import financial_balances as views

# bind ViewSets to URLs
financial_balances_agencies = views.AgenciesFinancialBalancesViewSet.as_view({"get": "list"})

urlpatterns = [re_path(r"^agencies/$", financial_balances_agencies)]
