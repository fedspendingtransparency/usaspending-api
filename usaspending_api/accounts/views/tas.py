from usaspending_api.accounts.models import TreasuryAppropriationAccount, AppropriationAccountBalances
from usaspending_api.accounts.serializers import TreasuryAppropriationAccountSerializer, AppropriationAccountBalancesSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.financial_activities.serializers import FinancialAccountsByProgramActivityObjectClassSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView, AggregateView
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
        queryset = AppropriationAccountBalances.final_objects.all()  # last per FY
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASBalancesAggregate(SuperLoggingMixin,
                           FilterQuerysetMixin,
                           AggregateView):
    """Return aggregated award information."""
    def get_queryset(self):
        queryset = AppropriationAccountBalances.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryList(SuperLoggingMixin,
                      FilterQuerysetMixin,
                      ResponseMetadatasetMixin,
                      DetailViewSet):
    """Handle requests for appropriation account balance information."""
    serializer_class = FinancialAccountsByProgramActivityObjectClassSerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryAggregate(SuperLoggingMixin,
                           FilterQuerysetMixin,
                           AggregateView):
    """Return aggregated award information."""
    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
