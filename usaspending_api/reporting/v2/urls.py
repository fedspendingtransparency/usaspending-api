from django.conf.urls import url
from usaspending_api.reporting.v2.views.placeholder import Placeholder

urlpatterns = [
    url(r"^placeholder/$", Placeholder.as_view()),
]
