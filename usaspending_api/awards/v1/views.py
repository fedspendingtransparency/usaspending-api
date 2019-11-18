from collections import namedtuple

from usaspending_api.awards.models import Award
from usaspending_api.awards.serializers import AwardSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.api_versioning import deprecated
from django.utils.decorators import method_decorator


AggregateItem = namedtuple("AggregateItem", ["field", "func"])


@method_decorator(deprecated, name="list")
@method_decorator(deprecated, name="retrieve")
class AwardAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    Return aggregated award information.
    """

    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = Award.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name="list")
@method_decorator(deprecated, name="retrieve")
class AwardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    ## Spending data by Award (i.e. a grant, contract, loan, etc)
    This endpoint allows you to search and filter by almost any attribute of an award object.
    """

    serializer_class = AwardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
