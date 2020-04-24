from django.conf.urls import url
from usaspending_api.agency.views import test

urlpatterns = [
    url(r"^test/$", test.EndpointTest.as_view()),
]
