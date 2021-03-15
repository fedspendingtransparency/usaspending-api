from django.conf.urls import url
from usaspending_api.recipient.v2.views.states import StateMetaDataViewSet, StateAwardBreakdownViewSet, ListStates
from usaspending_api.recipient.v2.views.recipients import RecipientOverView
from usaspending_api.recipient.v2.views.recipients import ChildRecipients
from usaspending_api.recipient.v2.views.duns.count import DunsCount
from usaspending_api.recipient.v2.views.list_recipients import ListRecipients

urlpatterns = [
    url(r"^duns/$", ListRecipients.as_view()),
    # This url is called interally by the ListRecipients view with a hardcoded path
    url(r"^duns/count/$", DunsCount.as_view()),
    url(r"^duns/(?P<recipient_id>.*)/$", RecipientOverView.as_view()),
    url(r"^children/(?P<duns>[0-9]{9})/$", ChildRecipients.as_view()),
    url(r"^state/(?P<fips>[0-9]{,2})/$", StateMetaDataViewSet.as_view()),
    url(r"^state/awards/(?P<fips>[0-9]{,2})/$", StateAwardBreakdownViewSet.as_view()),
    url(r"^state/$", ListStates.as_view()),
]
