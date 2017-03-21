from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.awards.serializers import FinancialAccountsByAwardsSerializer
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.common.mixins import SuperLoggingMixin


class TreasuryAppropriationAccountViewSet(SuperLoggingMixin,
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


class TreasuryAppropriationAccountAutocomplete(FilterQuerysetMixin,
                                               AutocompleteView):
    """Handle autocomplete requests for appropriation account (i.e., TAS) information."""
    serializer_class = TreasuryAppropriationAccountSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = TreasuryAppropriationAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        return filtered_queryset


class TreasuryAppropriationAccountBalancesViewSet(SuperLoggingMixin,
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


class FinancialAccountsByAwardListViewSet(
        SuperLoggingMixin,
        FilterQuerysetMixin,
        ResponseMetadatasetMixin,
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
