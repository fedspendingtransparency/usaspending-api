from django.conf.urls import url

from usaspending_api.download.v2.download_list_agencies import DownloadListAgenciesViewSet
from usaspending_api.download.v2.download_status import DownloadStatusViewSet
from usaspending_api.download.v2.list_monthly_downloads import ListMonthlyDownloadsViewset
from usaspending_api.download.v2.year_limited_downloads import YearLimitedDownloadViewSet

urlpatterns = [
    url(r"^awards", YearLimitedDownloadViewSet.as_view()),
    url(r"^status", DownloadStatusViewSet.as_view()),
    url(r"^list_agencies", DownloadListAgenciesViewSet.as_view()),
    url(r"^list_monthly_files", ListMonthlyDownloadsViewset.as_view()),
]
