from django.conf.urls import url

from usaspending_api.download.v2 import views


urlpatterns = [
    url(r'^awards', views.YearLimitedDownloadViewSet.as_view()),
    url(r'^status', views.DownloadStatusViewSet.as_view()),
    url(r'^list_agencies', views.DownloadListAgenciesViewSet.as_view()),
    url(r'^list_monthly_files', views.ListMonthlyDownloadsViewset.as_view())
]
