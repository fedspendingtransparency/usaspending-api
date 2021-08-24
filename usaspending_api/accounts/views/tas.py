from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.serializers import TasSerializer
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.common.mixins import FilterQuerysetMixin
from usaspending_api.common.mixins import AggregateQuerysetMixin
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.views import AutocompleteView
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.api_versioning import deprecated, removed
from usaspending_api.submissions.helpers import (
    get_latest_submission_ids_for_each_fiscal_quarter_file_a,
    get_latest_submission_ids_for_each_fiscal_quarter_file_b,
)
from django.utils.decorators import method_decorator


@method_decorator(deprecated, name="list")
class TASBalancesAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = AppropriationAccountBalances.objects.filter(submission__is_final_balances_for_fy=True)
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
        federal_account_id, fiscal_years = self.get_submission_id_filters()

        submission_ids = get_latest_submission_ids_for_each_fiscal_quarter_file_a(fiscal_years, federal_account_id)

        queryset = AppropriationAccountBalances.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = queryset.filter(submission_id__in=submission_ids)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TASCategoryAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = FinancialAccountsByProgramActivityObjectClass.objects.filter(
            submission__is_final_balances_for_fy=True
        )
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name="list")
class TASCategoryQuarterAggregate(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Handle requests for the latest quarter's financial data by appropriationappropriation
    account (tas), program activity, and object class.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        federal_account_id, fiscal_years = self.get_submission_id_filters()

        submission_ids = get_latest_submission_ids_for_each_fiscal_quarter_file_b(fiscal_years, federal_account_id)

        queryset = FinancialAccountsByProgramActivityObjectClass.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = queryset.filter(submission_id__in=submission_ids)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(removed, "post")
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
