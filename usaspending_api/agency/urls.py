from django.conf.urls import url
from usaspending_api.agency.views.program_activity_count import ProgramActivityCount
from usaspending_api.agency.views.federal_account_count import FederalAccountCount

urlpatterns = [
    url(r"^(?P<code>[0-9]{3,4})/program_activity/count/$", ProgramActivityCount.as_view()),
    url(r"^(?P<code>[0-9]{3,4})/federal_account/count/$", FederalAccountCount.as_view()),
]
