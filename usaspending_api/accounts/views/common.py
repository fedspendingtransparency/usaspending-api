from usaspending_api.awards.serializers import FinancialAccountsByAwardsSerializer
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet
from usaspending_api.common.serializers import AggregateSerializer


class FinancialAccountsByAwardAggregateViewSet(SuperLoggingMixin,
                                               FilterQuerysetMixin,
                                               AggregateQuerysetMixin,
                                               DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated FinancialAccountsByAward information."""
    def get_queryset(self):
        queryset = FinancialAccountsByAwards.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class FinancialAccountsByAwardListViewSet(
        SuperLoggingMixin,
        FilterQuerysetMixin,
        DetailViewSet):
    """
    Handles requests for financial account data grouped by award.
    """

    serializer_class = FinancialAccountsByAwardsSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = FinancialAccountsByAwards.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
