from django.conf.urls import url

from usaspending_api.accounts.views import tas as views

# bind ViewSets to URLs
tas_balances_quarters_total = views.TASBalancesQuarterAggregate.as_view({"get": "list", "post": "list"})
tas_balances_total = views.TASBalancesAggregate.as_view({"get": "list", "post": "list"})
tas_categories_total = views.TASCategoryAggregate.as_view({"get": "list", "post": "list"})
tas_categories_quarters_total = views.TASCategoryQuarterAggregate.as_view({"get": "list", "post": "list"})

urlpatterns = [
    url(r"^balances/total/", tas_balances_total),
    url(r"^balances/quarters/total/$", tas_balances_quarters_total),
    url(r"^categories/total/", tas_categories_total),
    url(r"^categories/quarters/total/$", tas_categories_quarters_total),
    url(r"^autocomplete/", views.TreasuryAppropriationAccountAutocomplete.as_view()),
]
