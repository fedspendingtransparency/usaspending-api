from django.conf.urls import url
from usaspending_api.recipient.v2.views.states import StateMetaDataViewSet, StateAwardBreakdownViewSet, \
    ListStates

urlpatterns = [
    url(r'^state/(?P<fips>[0-9]{,2})/$', StateMetaDataViewSet.as_view()),
    url(r'^state/awards/(?P<fips>[0-9]{,2})/$', StateAwardBreakdownViewSet.as_view()),
    url(r'^state/$', ListStates.as_view()),
]
