from django.urls import re_path

from usaspending_api.download.v2 import views
from usaspending_api.download.v2.download_count import DownloadTransactionCountViewSet
from usaspending_api.download.v2.download_status import DownloadStatusViewSet

urlpatterns = [
    re_path(r"^accounts", views.AccountDownloadViewSet.as_view()),
    re_path(r"^assistance", views.RowLimitedAssistanceDownloadViewSet.as_view()),
    re_path(r"^awards", views.RowLimitedAwardDownloadViewSet.as_view()),
    re_path(r"^contract", views.RowLimitedContractDownloadViewSet.as_view()),
    re_path(r"^count", DownloadTransactionCountViewSet.as_view()),
    re_path(r"^disaster/recipients", views.DisasterRecipientDownloadViewSet.as_view()),
    re_path(r"^disaster", views.DisasterDownloadViewSet.as_view()),
    re_path(r"^idv", views.RowLimitedIDVDownloadViewSet.as_view()),
    re_path(r"^status", DownloadStatusViewSet.as_view()),
    re_path(r"^transactions", views.RowLimitedTransactionDownloadViewSet.as_view()),
]
