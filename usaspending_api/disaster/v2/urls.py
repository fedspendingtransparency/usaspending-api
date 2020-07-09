from django.conf.urls import url

from usaspending_api.disaster.v2.views.agency.count import AgencyCountViewSet
from usaspending_api.disaster.v2.views.def_code.count import DefCodeCountViewSet
from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.federal_account.loans import Loans
from usaspending_api.disaster.v2.views.federal_account.spending import Spending
from usaspending_api.disaster.v2.views.object_class.count import ObjectClassCountViewSet
from usaspending_api.disaster.v2.views.overview import OverviewViewSet
from usaspending_api.disaster.v2.views.recipient.loans import RecipientLoans
from usaspending_api.disaster.v2.views.recipient.spending import RecipientSpending
from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet
from usaspending_api.disaster.v2.views.spending_by_geography import SpendingByGeographyViewSet

urlpatterns = [
    url(r"^agency/count/$", AgencyCountViewSet.as_view()),
    url(r"^def_code/count/$", DefCodeCountViewSet.as_view()),
    url(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    url(r"^federal_account/loans/$", Loans.as_view()),
    url(r"^federal_account/spending/$", Spending.as_view()),
    url(r"^object_class/count/$", ObjectClassCountViewSet.as_view()),
    url(r"^overview/$", OverviewViewSet.as_view()),
    url(r"^recipient/loans/$", RecipientLoans.as_view()),
    url(r"^recipient/spending/$", RecipientSpending.as_view()),
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
    url(r"^spending_by_geography/$", SpendingByGeographyViewSet.as_view()),
]
