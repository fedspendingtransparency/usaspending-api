from django.conf.urls import url

from usaspending_api.disaster_spending.v2.views.disaster_spending import SpendingExplorerViewSet

urlpatterns = [url(r"^$", SpendingExplorerViewSet.as_view())]
