from django.conf.urls import url

from usaspending_api.spending.v2.views.agency_explorer.spending_explorer import AgencyExplorerViewSet
from usaspending_api.spending.v2.views.budget_function_explorer.spending_explorer import BudgetFunctionExplorerViewSet
from usaspending_api.spending.v2.views.object_class_explorer.spending_explorer import ObjectClassExplorerViewSet
from usaspending_api.spending.v2.views.spending_explorer import SpendingExplorerViewSet

urlpatterns = [
    url(r'^', SpendingExplorerViewSet.as_view()),
    url(r'^budget', BudgetFunctionExplorerViewSet.as_view()),
    url(r'^agency', AgencyExplorerViewSet.as_view()),
    url(r'^object', ObjectClassExplorerViewSet.as_view())
]
