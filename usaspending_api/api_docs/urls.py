from django.urls import re_path
from usaspending_api.common.views import MarkdownView
from django_spaghetti.views import Plate


urlpatterns = [
    re_path(r"^$", MarkdownView.as_view(markdown="documentation_index.md")),
    re_path(r"^intro-tutorial", MarkdownView.as_view(markdown="api_tutorial.md")),
    re_path(r"^data-dictionary", MarkdownView.as_view(markdown="data_dictionary.md")),
    re_path(r"^recipes", MarkdownView.as_view(markdown="request_recipes.md")),
    re_path(r"^using-the-api", MarkdownView.as_view(markdown="using_the_api.md")),
    re_path(r"^entity-relationships", Plate.as_view(plate_template_name="plate.html"), name="plate"),
    re_path(r"^endpoints", MarkdownView.as_view(markdown="endpoints.md")),
]
