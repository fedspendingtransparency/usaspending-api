from django.urls import re_path

from usaspending_api.spending_explorer.v2.views.spending_explorer import SpendingExplorerViewSet

urlpatterns = [re_path(r"^$", SpendingExplorerViewSet.as_view())]
