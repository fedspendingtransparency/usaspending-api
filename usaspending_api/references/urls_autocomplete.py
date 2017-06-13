from django.conf.urls import url
from usaspending_api.references.views_v2.autocomplete import budget_function_autocomplete_view


urlpatterns = [
    url(r'^budget_function/', budget_function_autocomplete_view)
]
