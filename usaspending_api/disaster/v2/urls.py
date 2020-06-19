from django.conf.urls import url

from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet

urlpatterns = [url(r"^spending/$", ExperimentalDisasterViewSet.as_view())]
