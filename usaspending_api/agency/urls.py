from django.conf.urls import url
from usaspending_api.agency.views.program_activity_count import ProgramActivityCount

urlpatterns = [
    url(r"^(?P<pk>[0-9]+)/program_activity/count/$", ProgramActivityCount.as_view()),
]
