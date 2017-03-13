from django.conf import settings
from django.conf.urls import url, include
from usaspending_api import views as views
from usaspending_api.common.views import MarkdownView


urlpatterns = [
    url(r'^$', MarkdownView.as_view(markdown='documentation_index.md')),
    url(r'^intro-tutorial', MarkdownView.as_view(markdown='api_tutorial.md')),
    url(r'^data-dictionary', MarkdownView.as_view(markdown='data_dictionary.md')),
    url(r'^recipes', MarkdownView.as_view(markdown='request_recipes.md')),
    url(r'^using-the-api', MarkdownView.as_view(markdown='using_the_api.md')),
]
