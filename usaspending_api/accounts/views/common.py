from usaspending_api.awards.serializers import FinancialAccountsByAwardsSerializer
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.mixins import FilterQuerysetMixin
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.api_versioning import removed
from django.utils.decorators import method_decorator


@method_decorator(removed, name="list")
@method_decorator(removed, name="retrieve")
class FinancialAccountsByAwardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    Handles requests for financial account data grouped by award.
    """

    serializer_class = FinancialAccountsByAwardsSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = FinancialAccountsByAwards.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
