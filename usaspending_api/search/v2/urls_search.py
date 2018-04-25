from django.conf.urls import url
from usaspending_api.search.v2.views import search

urlpatterns = [
    url(r'^spending_over_time', search.SpendingOverTimeVisualizationViewSet.as_view()),
    # url(r'^spending_by_category', search.SpendingByCategoryVisualizationViewSet.as_view()), # will be used in future
    url(r'^spending_by_geography', search.SpendingByGeographyVisualizationViewSet.as_view()),
    url(r'^spending_by_award_count', search.SpendingByAwardCountVisualizationViewSet.as_view()),
    url(r'^spending_by_award', search.SpendingByAwardVisualizationViewSet.as_view()),
    url(r'^spending_by_transaction_count', search.SpendingByTransactionCountVisualizaitonViewSet.as_view()),
    url(r'^spending_by_transaction', search.SpendingByTransactionVisualizationViewSet.as_view()),
    url(r'^transaction_spending_summary', search.TransactionSummaryVisualizationViewSet.as_view())
]
