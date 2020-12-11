from django.conf.urls import url, include

urlpatterns = [url(r"^agencies/", include("usaspending_api.reporting.v2.views.agencies.urls"))]
