from django.conf.urls import url
from usaspending_api.search.v2.views import search_elasticsearch as es
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_award import SpendingByAwardVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_award_count import SpendingByAwardCountVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_category import SpendingByCategoryVisualizationViewSet
from usaspending_api.search.v2.views.spending_by_category_views.awarding_agency import AwardingAgencyViewSet
from usaspending_api.search.v2.views.spending_by_category_views.awarding_subagency import AwardingSubagencyViewSet
from usaspending_api.search.v2.views.spending_by_geography import SpendingByGeographyVisualizationViewSet
from usaspending_api.search.v2.views.spending_over_time import SpendingOverTimeVisualizationViewSet

urlpatterns = [
    url(r"^new_awards_over_time", NewAwardsOverTimeVisualizationViewSet.as_view()),
    url(r"^spending_by_award_count", SpendingByAwardCountVisualizationViewSet.as_view()),
    url(r"^spending_by_award", SpendingByAwardVisualizationViewSet.as_view()),
    url(r"^spending_by_category/awarding_agency", AwardingAgencyViewSet.as_view()),
    url(r"^spending_by_category/awarding_subagency", AwardingSubagencyViewSet.as_view()),
    url(r"^spending_by_category", SpendingByCategoryVisualizationViewSet.as_view()),
    url(r"^spending_by_geography", SpendingByGeographyVisualizationViewSet.as_view()),
    url(r"^spending_by_transaction_count", es.SpendingByTransactionCountVisualizaitonViewSet.as_view()),
    url(r"^spending_by_transaction", es.SpendingByTransactionVisualizationViewSet.as_view()),
    url(r"^spending_over_time", SpendingOverTimeVisualizationViewSet.as_view()),
    url(r"^transaction_spending_summary", es.TransactionSummaryVisualizationViewSet.as_view()),
]
