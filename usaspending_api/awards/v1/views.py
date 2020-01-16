from collections import namedtuple

from usaspending_api.awards.models import Award
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.views import CachedDetailViewSet
from usaspending_api.common.api_versioning import deprecated, removed
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


@method_decorator(removed, name="list")
@method_decorator(removed, name="retrieve")
class AwardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    REMOVED
    ## Spending data by Award (i.e. a grant, contract, loan, etc)
    This endpoint allows you to search and filter by almost any attribute of an award object.
    """

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        pass
