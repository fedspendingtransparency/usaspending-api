from django.urls import include, re_path

urlpatterns = [
    re_path(r"^agencies/", include("usaspending_api.reporting.v2.views.agencies.urls")),
]
