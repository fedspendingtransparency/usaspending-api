from django.conf.urls import url

from usaspending_api.accounts.v2.views import budget_function

urlpatterns = [
    url(r"^list_budget_functions/$", budget_function.ListBudgetFunctionViewSet.as_view()),
    url(r"^list_budget_subfunctions/$", budget_function.ListBudgetSubfunctionViewSet.as_view()),
]
