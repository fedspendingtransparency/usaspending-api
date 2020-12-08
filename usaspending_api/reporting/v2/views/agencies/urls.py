from django.conf.urls import url
from usaspending_api.reporting.v2.views.agencies.overview import AgenciesOverview

urlpatterns = [url(r"^overview/$", AgenciesOverview.as_view())]
