from django.conf.urls import url
from usaspending_api.search.v2.views.search import SpendingByAwardCountVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingByAwardVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingByCategoryVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingByGeographyVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingOverTimeVisualizationViewSet
from usaspending_api.search.v2.views.search import TransactionSummaryVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingByTransactionVisualizationViewSet
from usaspending_api.search.v2.views.search import SpendingByTransactionCountVisualizaitonViewSet

urlpatterns = [
    url(r'^spending_over_time', SpendingOverTimeVisualizationViewSet.as_view()),
    url(r'^spending_by_category', SpendingByCategoryVisualizationViewSet.as_view()),
    url(r'^spending_by_geography', SpendingByGeographyVisualizationViewSet.as_view()),
    url(r'^spending_by_award_count', SpendingByAwardCountVisualizationViewSet.as_view()),
    url(r'^spending_by_award', SpendingByAwardVisualizationViewSet.as_view()),
    url(r'^spending_by_transaction_count', SpendingByTransactionCountVisualizaitonViewSet.as_view()),
    url(r'^spending_by_transaction', SpendingByTransactionVisualizationViewSet.as_view()),
    url(r'^transaction_spending_summary', TransactionSummaryVisualizationViewSet.as_view())
]
