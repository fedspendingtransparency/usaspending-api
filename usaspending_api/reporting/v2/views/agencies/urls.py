from django.conf.urls import url
from usaspending_api.reporting.v2.views.differences import Differences

urlpatterns = [url(r"^(?P<toptier_code>[0-9]{3,4})/differences/$", Differences.as_view())]
