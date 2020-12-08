from django.conf.urls import url
from usaspending_api.reporting.v2.views.agencies.overview import AgenciesOverview
from usaspending_api.reporting.v2.views.agencies.agency_code.overview import AgencyOverview

urlpatterns = [
    url(r"^overview/$", AgenciesOverview.as_view()),
    url(r"^(?P<toptier_code>[0-9]{3,4})/overview/$", AgencyOverview.as_view())
]

