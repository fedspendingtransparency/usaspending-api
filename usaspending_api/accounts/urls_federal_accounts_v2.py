from django.urls import re_path

from usaspending_api.accounts.views import federal_accounts_v2 as views

# bind ViewSets to URLs
object_class_federal_accounts = views.ObjectClassFederalAccountsViewSet.as_view()
fiscal_year_snapshot_federal_accounts = views.FiscalYearSnapshotFederalAccountsViewSet.as_view()
spending_over_time_federal_accounts = views.SpendingOverTimeFederalAccountsViewSet.as_view()
spending_by_category_federal_accounts = views.SpendingByCategoryFederalAccountsViewSet.as_view()
federal_account = views.FederalAccountViewSet.as_view()
federal_accounts = views.FederalAccountsViewSet.as_view()

urlpatterns = [
    re_path(r"(?P<pk>[0-9]+)/available_object_classes/?$", object_class_federal_accounts),
    re_path(r"(?P<pk>[0-9]+)/fiscal_year_snapshot/(?P<fy>[0-9]+)/?$", fiscal_year_snapshot_federal_accounts),
    re_path(r"(?P<pk>[0-9]+)/fiscal_year_snapshot/?$", fiscal_year_snapshot_federal_accounts),
    # re_path(r'(?P<pk>[0-9]+)/spending_over_time$', spending_over_time_federal_accounts),
    # re_path(r'(?P<pk>[0-9]+)/spending_by_category$', spending_by_category_federal_accounts),
    re_path(r"(?P<federal_account_code>[0-9]{3}[\-][0-9]{4})/$", federal_account),
    re_path(r"$", federal_accounts),
]
