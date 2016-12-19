from django.conf.urls import url
from usaspending_api.references import views

urlpatterns = [
    url(r'^locations/$', views.LocationEndpoint.as_view()),
    url(r'^locations/geocomplete', views.LocationEndpoint.as_view(), {'geocomplete': True})
]
