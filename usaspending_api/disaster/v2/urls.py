from django.conf.urls import url

from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet
from usaspending_api.disaster.v2.views.federal_account.spending import Spending, Loans

urlpatterns = [
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
    url(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    url(r"^federal_account/loans/$", Loans.as_view()),
    url(r"^federal_account/spending/$", Spending.as_view()),
]
