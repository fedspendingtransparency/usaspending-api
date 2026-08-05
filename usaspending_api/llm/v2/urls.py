from django.urls import path

from usaspending_api.llm.v2.views.filter_search import FilterSearchViewSet

urlpatterns = [
    path("filter-search/", FilterSearchViewSet.as_view()),
]
