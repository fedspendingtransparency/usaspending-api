from django.urls import re_path
from usaspending_api.reporting.v2.views.agencies.toptier_code.discrepancies import AgencyDiscrepancies
from usaspending_api.reporting.v2.views.agencies.toptier_code.overview import AgencyOverview
from usaspending_api.reporting.v2.views.agencies.overview import AgenciesOverview
from usaspending_api.reporting.v2.views.agencies.toptier_code.differences import Differences
from usaspending_api.reporting.v2.views.submission_history import SubmissionHistory
from usaspending_api.reporting.v2.views.agencies.toptier_code.fiscal_year.fiscal_period.unlinked_awards import (
    UnlinkedAwards,
)

from usaspending_api.reporting.v2.views.agencies.publish_dates import PublishDates

urlpatterns = [
    re_path(r"^overview/$", AgenciesOverview.as_view()),
    re_path(r"^(?P<toptier_code>[0-9]{3,4})/differences/$", Differences.as_view()),
    re_path(r"^(?P<toptier_code>[0-9]{3,4})/discrepancies/$", AgencyDiscrepancies.as_view()),
    re_path(r"^(?P<toptier_code>[0-9]{3,4})/overview/$", AgencyOverview.as_view()),
    re_path(
        r"^(?P<toptier_code>[0-9]{3,4})/(?P<fiscal_year>[0-9]{4})/(?P<fiscal_period>[0-9]{1,2})/submission_history/$",
        SubmissionHistory.as_view(),
    ),
    re_path(
        r"^(?P<toptier_code>[0-9]{3,4})/(?P<fiscal_year>[0-9]{4})/(?P<fiscal_period>[0-9]{1,2})/unlinked_awards/(?P<type>[\w]+)/$",
        UnlinkedAwards.as_view(),
    ),
    re_path(r"^publish_dates/$", PublishDates.as_view()),
]
