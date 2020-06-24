from django.conf.urls import url

from usaspending_api.disaster.v2.views.agency.count import AgencyCountViewSet
from usaspending_api.disaster.v2.views.def_code.count import DefCodeCountViewSet
from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet

urlpatterns = [
    url(r"^agency/count/$", AgencyCountViewSet.as_view()),
    url(r"^def_code/count/$", DefCodeCountViewSet.as_view()),
    url(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
]
