from django.conf.urls import url
from usaspending_api.recipient.v2.views import StateMetaDataViewSet

urlpatterns = [
    url(r'^state/(?P<fips>[0-9]{2})/$', StateMetaDataViewSet.as_view())
]
