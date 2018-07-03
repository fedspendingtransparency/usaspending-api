from django.conf.urls import url
from usaspending_api.recipient.v2.views.states import StateMetaDataViewSet, StateAwardBreakdownViewSet, \
    ListStates
from usaspending_api.recipient.v2.views.recipients import \
    RecipientOverView  # , ListRecipients, ChildRecipients, RecipientSearch, RecipientAwardsOverTime

urlpatterns = [
    url(r'^recipient/duns/(?P<duns>[0-9]{9})/$', RecipientOverView.as_view()),
    # url(r'^recipient/list/(?P<duns>[0-9]{9})/$', ListRecipients.as_view()),
    # url(r'^recipient/children/(?P<duns>[0-9]{9})/$', ChildRecipients.as_view())

    url(r'^state/(?P<fips>[0-9]{,2})/$', StateMetaDataViewSet.as_view()),
    url(r'^state/awards/(?P<fips>[0-9]{,2})/$', StateAwardBreakdownViewSet.as_view()),
    url(r'^state/$', ListStates.as_view()),
]
