from django.urls import include, path, re_path
from usaspending_api.agency.v2.views.agency_overview import AgencyOverview
from usaspending_api.agency.v2.views.object_class_count import ObjectClassCount
from usaspending_api.agency.v2.views.program_activity_count import ProgramActivityCount


urlpatterns = [
    re_path(
        "(?P<toptier_code>[0-9]{3,4})/",
        include(
            [
                path("", AgencyOverview.as_view()),
                path("object_class/count/", ObjectClassCount.as_view()),
                path("program_activity/count/", ProgramActivityCount.as_view()),
            ]
        ),
    )
]
