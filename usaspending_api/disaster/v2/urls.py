from django.conf.urls import url

from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet
from usaspending_api.disaster.v2.views.overview import OverviewViewSet

urlpatterns = [
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
    url(r"^overview/$", OverviewViewSet.as_view()),
]
