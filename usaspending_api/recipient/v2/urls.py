from django.urls import re_path
from usaspending_api.recipient.v2.views.states import StateMetaDataViewSet, StateAwardBreakdownViewSet, ListStates
from usaspending_api.recipient.v2.views.recipients import RecipientOverViewDuns, RecipientOverView
from usaspending_api.recipient.v2.views.recipients import ChildRecipients
from usaspending_api.recipient.v2.views.list_recipients import ListRecipients, ListRecipientsByDuns, RecipientCount

urlpatterns = [
    re_path(r"^$", ListRecipients.as_view()),
    re_path(r"^duns/$", ListRecipientsByDuns.as_view()),
    re_path(r"^count/$", RecipientCount.as_view()),
    # Regex below accepts 9 numberic char DUNS or 12 alpha-numeric char UEI
    re_path(r"^children/(?P<duns_or_uei>(?:[0-9]{9}|[0-9a-zA-Z]{12}))/$", ChildRecipients.as_view()),
    re_path(r"^duns/(?P<recipient_id>.*)/$", RecipientOverViewDuns.as_view()),
    re_path(r"^state/(?P<fips>[0-9]{,2})/$", StateMetaDataViewSet.as_view()),
    re_path(r"^state/awards/(?P<fips>[0-9]{,2})/$", StateAwardBreakdownViewSet.as_view()),
    re_path(r"^state/$", ListStates.as_view()),
    re_path(r"^(?P<recipient_id>.*)/$", RecipientOverView.as_view()),
]
