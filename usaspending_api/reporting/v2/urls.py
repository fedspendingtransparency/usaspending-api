from django.conf.urls import url, include
from usaspending_api.reporting.v2.views.placeholder import Placeholder

urlpatterns = [
    url(r"^placeholder/$", Placeholder.as_view()),
    url(r"^agencies/", include("usaspending_api.reporting.v2.views.agencies.urls")),
]
