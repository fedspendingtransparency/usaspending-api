from django.conf.urls import url
from usaspending_api.common.views import MarkdownView
from django_spaghetti.views import Plate


urlpatterns = [
    url(r"^$", MarkdownView.as_view(markdown="documentation_index.md")),
    url(r"^intro-tutorial", MarkdownView.as_view(markdown="api_tutorial.md")),
    url(r"^data-dictionary", MarkdownView.as_view(markdown="data_dictionary.md")),
    url(r"^recipes", MarkdownView.as_view(markdown="request_recipes.md")),
    url(r"^using-the-api", MarkdownView.as_view(markdown="using_the_api.md")),
    url(r"^entity-relationships", Plate.as_view(plate_template_name="plate.html"), name="plate"),
    url(r"^endpoints", MarkdownView.as_view(markdown="endpoints.md")),
]
