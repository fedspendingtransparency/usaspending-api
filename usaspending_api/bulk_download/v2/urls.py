from django.conf.urls import url

from usaspending_api.bulk_download.v2 import views
# from usaspending_api.download.v2.views.common import DownloadStatusViewSet, DownloadColumnsViewSet


urlpatterns = [
    url(r'^awards', views.BulkDownloadAwardsViewSet.as_view()),
    url(r'^status', views.BulkDownloadStatusViewSet.as_view()),
    url(r'^list_agencies', views.BulkDownloadListAgenciesViewSet.as_view()),
    url(r'^list_monthly_files', views.ListMonthylDownloadsViewset.as_view())
]
