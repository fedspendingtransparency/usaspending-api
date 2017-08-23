from django.conf.urls import url
from usaspending_api.search.v2.views.visualizations import SpendingOverTimeVisualizationViewSet, \
    SpendingByCategoryVisualizationViewSet

urlpatterns = [
    url(r'^spending_over_time', SpendingOverTimeVisualizationViewSet.as_view()),
    url(r'^spending_by_category', SpendingByCategoryVisualizationViewSet.as_view())
]
