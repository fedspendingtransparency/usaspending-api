from django.conf.urls import url

from usaspending_api.download.v2 import views
# from usaspending_api.download.v2.views.common import DownloadStatusViewSet, DownloadColumnsViewSet


urlpatterns = [
    url(r'^awards', views.RowLimitedAwardDownloadViewSet.as_view()),
    # url(r'^columns', DownloadColumnsViewSet.as_view()),
    url(r'^status', views.DownloadStatusViewSet.as_view()),
    url(r'^transactions', views.RowLimitedTransactionDownloadViewSet.as_view()),
    # Note: This is commented out for now as it may be used in the near future
    # url(r'^subawards', views.RowLimitedSubawardDownloadViewSet.as_view()),
    url(r'^count', views.DownloadTransactionCountViewSet.as_view())
]
