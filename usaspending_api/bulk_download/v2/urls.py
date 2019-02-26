from django.conf.urls import url

from usaspending_api.download.v2 import views
from usaspending_api.download.v2.download_list_agencies import DownloadListAgenciesViewSet
from usaspending_api.download.v2.list_monthly_downloads import ListMonthlyDownloadsViewset

urlpatterns = [
    url(r'^awards', views.YearLimitedDownloadViewSet.as_view()),
    url(r'^status', views.DownloadStatusViewSet.as_view()),
    url(r'^list_agencies', DownloadListAgenciesViewSet.as_view()),
    url(r'^list_monthly_files', ListMonthlyDownloadsViewset.as_view())
]
