from django.conf.urls import url
from usaspending_api.agency.views.program_activity_count import ProgramActivityCount

urlpatterns = [
    url(r"^(?P<code>[0-9]{3,4})/program_activity/count/$", ProgramActivityCount.as_view()),
]
