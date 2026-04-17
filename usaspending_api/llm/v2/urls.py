from django.urls import path
from usaspending_api.llm.v2.views.search import StreamingSearchView, FeedbackView

app_name = "search_assistant"

urlpatterns = [
    path("filter-search/", StreamingSearchView.as_view(), name="streaming_search"),
    path("session/<int:session_id>/feedback/", FeedbackView.as_view(), name="feedback"),
]
