from usaspending_api.accounts.models import (
    TreasuryAppropriationAccount, AppropriationAccountBalances,
    AppropriationAccountBalancesQuarterly)
from usaspending_api.accounts.serializers import (
    AppropriationAccountBalancesSerializer,
    TasCategorySerializer,
    TasSerializer)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass,
    TasProgramActivityObjectClassQuarterly)
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.common.mixins import SuperLoggingMixin
from usaspending_api.common.serializers import AggregateSerializer


class TreasuryAppropriationAccountViewSet(SuperLoggingMixin,
                                          FilterQuerysetMixin,
                                          DetailViewSet):
    """Handle requests for appropriation account (i.e., TAS) information."""
    serializer_class = TasSerializer

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
    serializer_class = TasSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = TreasuryAppropriationAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        return filtered_queryset


class TreasuryAppropriationAccountBalancesViewSet(SuperLoggingMixin,
                                                  FilterQuerysetMixin,
                                                  DetailViewSet):
    """Handle requests for appropriation account balance information."""
    serializer_class = AppropriationAccountBalancesSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.final_objects.all()  # last per FY
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASBalancesQuarterList(SuperLoggingMixin,
                             FilterQuerysetMixin,
                             DetailViewSet):
    """
    Handle requests for quarterly financial data by appropriationappropriation
    account (tas)..
    """
    serializer_class = AppropriationAccountBalancesSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalancesQuarterly.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASBalancesQuarterAggregate(SuperLoggingMixin,
                                  FilterQuerysetMixin,
                                  AggregateQuerysetMixin,
                                  DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated award information."""
    def get_queryset(self):
        queryset = AppropriationAccountBalancesQuarterly.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASBalancesAggregate(SuperLoggingMixin,
                           FilterQuerysetMixin,
                           AggregateQuerysetMixin,
                           DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated award information."""
    def get_queryset(self):
        queryset = AppropriationAccountBalances.final_objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASCategoryList(SuperLoggingMixin,
                      FilterQuerysetMixin,
                      DetailViewSet):
    """Handle requests for appropriation account balance information."""
    serializer_class = TasCategorySerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryAggregate(SuperLoggingMixin,
                           FilterQuerysetMixin,
                           AggregateQuerysetMixin,
                           DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated award information."""
    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASCategoryQuarterList(SuperLoggingMixin,
                             FilterQuerysetMixin,
                             DetailViewSet):
    """
    Handle requests for quarterly financial data by appropriationappropriation
    account (tas), program activity, and object class.
    """
    serializer_class = TasCategorySerializer

    def get_queryset(self):
        queryset = TasProgramActivityObjectClassQuarterly.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryQuarterAggregate(SuperLoggingMixin,
                                  FilterQuerysetMixin,
                                  AggregateQuerysetMixin,
                                  DetailViewSet):
    """
    Handle requests for quarterly financial data by appropriationappropriation
    account (tas), program activity, and object class.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = TasProgramActivityObjectClassQuarterly.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset
