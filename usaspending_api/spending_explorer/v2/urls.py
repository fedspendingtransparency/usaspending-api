from django.conf.urls import url

from usaspending_api.spending.v2.views.spending_explorer import SpendingExplorerViewSet

urlpatterns = [
    url(r'^$', SpendingExplorerViewSet.as_view())
]
