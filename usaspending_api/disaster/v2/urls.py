from django.conf.urls import url

from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.object_class.count import ObjectClassCountViewSet
from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet

urlpatterns = [
    url(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    url(r"^object_class/count/$", ObjectClassCountViewSet.as_view()),
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
]
