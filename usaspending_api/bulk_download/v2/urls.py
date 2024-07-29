from django.urls import re_path

from usaspending_api.download.v2.download_list_agencies import DownloadListAgenciesViewSet
from usaspending_api.download.v2.download_status import DownloadStatusViewSet
from usaspending_api.download.v2.list_database_downloads import ListDatabaseDownloadsViewSet
from usaspending_api.download.v2.list_monthly_downloads import ListMonthlyDownloadsViewSet
from usaspending_api.download.v2.list_unlinked_awards_downloads import ListUnlinkedAwardsDownloadsViewSet
from usaspending_api.download.v2.year_limited_downloads import YearLimitedDownloadViewSet

urlpatterns = [
    re_path(r"^awards", YearLimitedDownloadViewSet.as_view()),
    re_path(r"^list_agencies", DownloadListAgenciesViewSet.as_view()),
    re_path(r"^list_monthly_files", ListMonthlyDownloadsViewSet.as_view()),
    re_path(r"^list_unlinked_awards_files", ListUnlinkedAwardsDownloadsViewSet.as_view()),
    re_path(r"^list_database_download_files", ListDatabaseDownloadsViewSet.as_view()),
    re_path(r"^status", DownloadStatusViewSet.as_view()),
]
