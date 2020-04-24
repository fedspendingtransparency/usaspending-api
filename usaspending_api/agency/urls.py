from django.conf.urls import url
from usaspending_api.agency.views import test, program_activity_count

urlpatterns = [
    url(r"^test/$", test.EndpointTest.as_view()),
    url(r"^(P<pk>[0-9]+)/program_activity/count$", program_activity_count.as_view()),
]
