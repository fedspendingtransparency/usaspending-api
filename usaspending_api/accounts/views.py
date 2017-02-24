from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import DetailViewSet
from rest_framework_tracking.mixins import LoggingMixin


class TreasuryAppropriationAccountViewSet(LoggingMixin,
                                          FilterQuerysetMixin,
                                          ResponseMetadatasetMixin,
                                          DetailViewSet):
    """Handle requests for appropriation account (i.e., TAS) information."""
    serializer_class = TreasuryAppropriationAccountSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = TreasuryAppropriationAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TreasuryAppropriationAccountBalancesViewSet(LoggingMixin,
                                                  FilterQuerysetMixin,
                                                  ResponseMetadatasetMixin,
                                                  DetailViewSet):
    """Handle requests for appropriation account balance information."""
    serializer_class = AppropriationAccountBalancesSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
