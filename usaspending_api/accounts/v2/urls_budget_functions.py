from django.urls import re_path

from usaspending_api.accounts.v2.views import budget_function

urlpatterns = [
    re_path(r"^list_budget_functions/$", budget_function.ListBudgetFunctionViewSet.as_view()),
    re_path(r"^list_budget_subfunctions/$", budget_function.ListBudgetSubfunctionViewSet.as_view()),
]
