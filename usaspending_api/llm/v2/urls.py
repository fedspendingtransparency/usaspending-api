from django.urls import path
from usaspending_api.llm.v2.views.search import StreamingSearchView

app_name = "search_assistant"

urlpatterns = [path("filter-search/", StreamingSearchView.as_view(), name="streaming_search")]
