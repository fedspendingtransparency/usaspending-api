from django.conf.urls import url

from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet

urlpatterns = [
    url(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
]
