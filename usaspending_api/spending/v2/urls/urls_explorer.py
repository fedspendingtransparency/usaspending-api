from django.conf.urls import url
from usaspending_api.spending.v2.views.spending_explorer import SpendingExplorerViewSet

urlpatterns = [
    url(r'^spending_explorer', SpendingExplorerViewSet.as_view())
]