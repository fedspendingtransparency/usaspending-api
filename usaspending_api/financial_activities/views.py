from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.financial_activities.serializers import FinancialAccountsByProgramActivityObjectClassSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin, SuperLoggingMixin
from usaspending_api.common.views import DetailViewSet


class FinancialAccountsByProgramActivityObjectClassListViewSet(
        SuperLoggingMixin,
        FilterQuerysetMixin,
        ResponseMetadatasetMixin,
        DetailViewSet):
    """
    Handles requests for financial account data grouped by program
    activity and object class.
    """

    serializer_class = FinancialAccountsByProgramActivityObjectClassSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
