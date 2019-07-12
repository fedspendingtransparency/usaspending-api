from django.conf.urls import url

from usaspending_api.accounts.views import common as views

# bind ViewSets to URLs
financial_accounts_by_award = views.FinancialAccountsByAwardListViewSet.as_view({"get": "list", "post": "list"})
financial_accounts_by_award_detail = views.FinancialAccountsByAwardListViewSet.as_view(
    {"get": "retrieve", "post": "retrieve"}
)
financial_accounts_by_award_total = views.FinancialAccountsByAwardAggregateViewSet.as_view(
    {"get": "list", "post": "list"}
)

urlpatterns = [
    url(r"^awards/$", financial_accounts_by_award),
    url(r"^awards/total/$", financial_accounts_by_award_total, name="awards-total"),
    url(r"^awards/(?P<pk>[0-9]+)/$", financial_accounts_by_award_detail),
]
