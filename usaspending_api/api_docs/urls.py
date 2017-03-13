from django.conf import settings
from django.conf.urls import url, include
from usaspending_api import views as views
from usaspending_api.common.views import MarkdownView


urlpatterns = [
    url(r'^$', MarkdownView.as_view(markdown='documentation_index.md')),
    url(r'^docs/tutorial', MarkdownView.as_view(markdown='api_tutorial.md')),
    url(r'^docs/data-dictionary', MarkdownView.as_view(markdown='data_dictionary.md')),
    url(r'^docs/recipies', MarkdownView.as_view(markdown='request_recipies.md')),
]
