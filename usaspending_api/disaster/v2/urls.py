from django.conf.urls import url

from usaspending_api.disaster.v2.views.spending import ExperimentalDisasterViewSet
from usaspending_api.disaster.v2.views import federal_account as fa

urlpatterns = [
    url(r"^spending/$", ExperimentalDisasterViewSet.as_view()),
    url(r"^federal_account/loans/$", fa.Loans.as_view()),
    url(r"^federal_account/spending/$", fa.Spending.as_view()),
]
