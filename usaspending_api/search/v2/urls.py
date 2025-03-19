from django.urls import include, re_path

from usaspending_api.search.v2.views import search_elasticsearch as es
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_award import SpendingByAwardVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_award_count import SpendingByAwardCountVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_category import SpendingByCategoryVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_geography import SpendingByGeographyVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_transaction_grouped import (
    SpendingByTransactionGroupedVisualizationViewSet,
)
from usaspending_api.search.v2.views.spending_over_time import SpendingOverTimeVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_subaward_grouped import SpendingBySubawardGroupedVisualizationViewSet

urlpatterns = [
    re_path(r"^new_awards_over_time", NewAwardsOverTimeVisualizationViewSet.as_view()),
    re_path(r"^spending_by_award_count", SpendingByAwardCountVisualizationViewSet.as_view()),
    re_path(r"^spending_by_award", SpendingByAwardVisualizationViewSet.as_view()),
    re_path(r"^spending_by_category/", include("usaspending_api.search.v2.urls_spending_by_category")),
    re_path(r"^spending_by_category$", SpendingByCategoryVisualizationViewSet.as_view()),
    re_path(r"^spending_by_geography", SpendingByGeographyVisualizationViewSet.as_view()),
    re_path(r"^spending_by_subaward_grouped", SpendingBySubawardGroupedVisualizationViewSet.as_view()),
    re_path(r"^spending_by_transaction_count", es.SpendingByTransactionCountVisualizationViewSet.as_view()),
    re_path(r"^spending_by_transaction_grouped", SpendingByTransactionGroupedVisualizationViewSet.as_view()),
    re_path(r"^spending_by_transaction", es.SpendingByTransactionVisualizationViewSet.as_view()),
    re_path(r"^spending_over_time", SpendingOverTimeVisualizationViewSet.as_view()),
    re_path(r"^transaction_spending_summary", es.TransactionSummaryVisualizationViewSet.as_view()),
]
