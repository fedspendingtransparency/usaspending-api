from django.urls import re_path

from usaspending_api.accounts.views import financial_spending as views

# bind ViewSets to URLs
object_class_financial_spending = views.ObjectClassFinancialSpendingViewSet.as_view({"get": "list"})
minor_object_class_financial_spending = views.MinorObjectClassFinancialSpendingViewSet.as_view({"get": "list"})

urlpatterns = [
    re_path(r"^major_object_class/$", object_class_financial_spending),
    re_path(r"^object_class/$", minor_object_class_financial_spending),
]
