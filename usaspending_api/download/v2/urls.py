from django.conf.urls import url
from usaspending_api.download.v2.views.award import DownloadAwardsViewSet
from usaspending_api.download.v2.views.common import DownloadStatusViewSet, DownloadColumnsViewSet
from usaspending_api.download.v2.views.transaction import DownloadTransactionsViewSet


urlpatterns = [
    url(r'^awards', DownloadAwardsViewSet.as_view()),
    url(r'^columns', DownloadColumnsViewSet.as_view()),
    url(r'^status', DownloadStatusViewSet.as_view()),
    url(r'^transactions', DownloadTransactionsViewSet.as_view())
]