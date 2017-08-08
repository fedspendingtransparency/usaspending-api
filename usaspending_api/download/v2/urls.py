from django.conf.urls import url
from usaspending_api.download.v2.views.award import DownloadAwardViewSet
from usaspending_api.download.v2.views.common import DownloadStatusViewSet, DownloadColumnsViewSet
from usaspending_api.download.v2.views.transaction import DownloadTransactionViewSet


urlpatterns = [
    url(r'^award', DownloadAwardViewSet.as_view()),
    url(r'^columns', DownloadColumnsViewSet.as_view()),
    url(r'^status', DownloadStatusViewSet.as_view()),
    url(r'^transaction', DownloadTransactionViewSet.as_view())
]