from django.conf.urls import url

from usaspending_api.download.v2 import views
from usaspending_api.download.v2.download_transaction_count import DownloadTransactionCountViewSet


urlpatterns = [
    url(r'^accounts', views.AccountDownloadViewSet.as_view()),
    url(r'^awards', views.RowLimitedAwardDownloadViewSet.as_view()),
    url(r'^count', DownloadTransactionCountViewSet.as_view()),
    url(r'^status', views.DownloadStatusViewSet.as_view()),
    url(r'^transactions', views.RowLimitedTransactionDownloadViewSet.as_view()),
]
