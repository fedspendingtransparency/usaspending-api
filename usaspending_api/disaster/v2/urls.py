from django.urls import re_path

from usaspending_api.disaster.v2.views.agency.count import AgencyCountViewSet
from usaspending_api.disaster.v2.views.agency.loans import route_agency_loans_backend
from usaspending_api.disaster.v2.views.agency.spending import route_agency_spending_backend
from usaspending_api.disaster.v2.views.award.amount import AmountViewSet
from usaspending_api.disaster.v2.views.cfda.count import CfdaCountViewSet
from usaspending_api.disaster.v2.views.cfda.loans import CfdaLoansViewSet
from usaspending_api.disaster.v2.views.cfda.spending import CfdaSpendingViewSet
from usaspending_api.disaster.v2.views.def_code.count import DefCodeCountViewSet
from usaspending_api.disaster.v2.views.federal_account.count import FederalAccountCountViewSet
from usaspending_api.disaster.v2.views.federal_account.loans import LoansViewSet
from usaspending_api.disaster.v2.views.federal_account.spending import SpendingViewSet
from usaspending_api.disaster.v2.views.object_class.count import ObjectClassCountViewSet
from usaspending_api.disaster.v2.views.object_class.loans import ObjectClassLoansViewSet
from usaspending_api.disaster.v2.views.object_class.spending import ObjectClassSpendingViewSet
from usaspending_api.disaster.v2.views.overview import OverviewViewSet
from usaspending_api.disaster.v2.views.recipient.count import RecipientCountViewSet
from usaspending_api.disaster.v2.views.recipient.loans import RecipientLoansViewSet
from usaspending_api.disaster.v2.views.recipient.spending import RecipientSpendingViewSet
from usaspending_api.disaster.v2.views.spending_by_geography import SpendingByGeographyViewSet

urlpatterns = [
    re_path(r"^agency/count/$", AgencyCountViewSet.as_view()),
    re_path(r"^agency/loans/$", route_agency_loans_backend()),
    re_path(r"^agency/spending/$", route_agency_spending_backend()),
    re_path(r"^award/amount/$", AmountViewSet.as_view(count_only=False)),
    re_path(r"^award/count/$", AmountViewSet.as_view(count_only=True)),
    re_path(r"^cfda/count/$", CfdaCountViewSet.as_view()),
    re_path(r"^cfda/loans/$", CfdaLoansViewSet.as_view()),
    re_path(r"^cfda/spending/$", CfdaSpendingViewSet.as_view()),
    re_path(r"^def_code/count/$", DefCodeCountViewSet.as_view()),
    re_path(r"^federal_account/count/$", FederalAccountCountViewSet.as_view()),
    re_path(r"^federal_account/loans/$", LoansViewSet.as_view()),
    re_path(r"^federal_account/spending/$", SpendingViewSet.as_view()),
    re_path(r"^object_class/count/$", ObjectClassCountViewSet.as_view()),
    re_path(r"^object_class/loans/$", ObjectClassLoansViewSet.as_view()),
    re_path(r"^object_class/spending/$", ObjectClassSpendingViewSet.as_view()),
    re_path(r"^overview/$", OverviewViewSet.as_view()),
    re_path(r"^recipient/count/$", RecipientCountViewSet.as_view()),
    re_path(r"^recipient/loans/$", RecipientLoansViewSet.as_view()),
    re_path(r"^recipient/spending/$", RecipientSpendingViewSet.as_view()),
    re_path(r"^spending_by_geography/$", SpendingByGeographyViewSet.as_view()),
]
