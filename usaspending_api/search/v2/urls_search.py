from django.conf.urls import url
from usaspending_api.search.v2.views import search
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_category import SpendingByCategoryVisualizationViewSet
from usaspending_api.search.v2.views.spending_over_time import SpendingOverTimeVisualizationViewSet

urlpatterns = [
    url(r'^new_awards_over_time', NewAwardsOverTimeVisualizationViewSet.as_view()),
    url(r'^spending_over_time', SpendingOverTimeVisualizationViewSet.as_view()),
    url(r'^spending_by_category', SpendingByCategoryVisualizationViewSet.as_view()),
    url(r'^spending_by_geography', search.SpendingByGeographyVisualizationViewSet.as_view()),
    url(r'^spending_by_award_count', search.SpendingByAwardCountVisualizationViewSet.as_view()),
    url(r'^spending_by_award', search.SpendingByAwardVisualizationViewSet.as_view()),
    url(r'^spending_by_transaction_count', search.SpendingByTransactionCountVisualizaitonViewSet.as_view()),
    url(r'^spending_by_transaction', search.SpendingByTransactionVisualizationViewSet.as_view()),
    url(r'^transaction_spending_summary', search.TransactionSummaryVisualizationViewSet.as_view())
]
