from django.conf.urls import url
from usaspending_api.search.v2.views.visualizations import SpendingOverTimeVisualizationViewSet

urlpatterns = [
    url(r'^spending_over_time', SpendingOverTimeVisualizationViewSet.as_view())
    ]
