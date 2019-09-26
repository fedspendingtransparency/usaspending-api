from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import AppropriationAccountBalancesSerializer
from usaspending_api.accounts.serializers import TasCategorySerializer
from usaspending_api.accounts.serializers import TasSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.common.mixins import FilterQuerysetMixin
from usaspending_api.common.mixins import AggregateQuerysetMixin
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.views import AutocompleteView
from usaspending_api.common.serializers import AggregateSerializer


class TASBalancesAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.final_objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASBalancesQuarterAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASBalancesQuarterList(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for the latest quarter's financial data by appropriationappropriation
    account (tas)..
    """

    serializer_class = AppropriationAccountBalancesSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASCategoryList(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for appropriation account balance information.
    """

    serializer_class = TasCategorySerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.final_objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TASCategoryQuarterAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for the latest quarter's financial data by appropriationappropriation
    account (tas), program activity, and object class.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASCategoryQuarterList(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for the latest quarter's financial data by appropriationappropriation
    account (tas), program activity, and object class.
    """

    serializer_class = TasCategorySerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TreasuryAppropriationAccountAutocomplete(FilterQuerysetMixin, AutocompleteView):
    """
    Handle autocomplete requests for appropriation account (i.e., TAS) information.
    """

    serializer_class = TasSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = TreasuryAppropriationAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TreasuryAppropriationAccountBalancesViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for appropriation account balance information.
    """

    serializer_class = AppropriationAccountBalancesSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.final_objects.all()  # last per FY
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TreasuryAppropriationAccountViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for appropriation account (i.e., TAS) information.
    """

    serializer_class = TasSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = TreasuryAppropriationAccount.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
