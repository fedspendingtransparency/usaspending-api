from django.conf.urls import url
from usaspending_api.search.v2.views.search import SpendingOverTimeVisualizationViewSet, \
    SpendingByCategoryVisualizationViewSet, SpendingByGeographyVisualizationViewSet

urlpatterns = [
    url(r'^spending_over_time', SpendingOverTimeVisualizationViewSet.as_view()),
    url(r'^spending_by_category', SpendingByCategoryVisualizationViewSet.as_view()),
    url(r'^spending_by_geography', SpendingByGeographyVisualizationViewSet.as_view())
]
