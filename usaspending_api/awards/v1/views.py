from collections import namedtuple

from usaspending_api.awards.models import Award, Subaward
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.serializers import AwardSerializer, SubawardSerializer, TransactionNormalizedSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.views import DetailViewSet, CachedDetailViewSet, AutocompleteView
from usaspending_api.common.api_versioning import deprecated
from django.utils.decorators import method_decorator


AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
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


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class AwardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    ## Spending data by Award (i.e. a grant, contract, loan, etc)
    This endpoint allows you to search and filter by almost any attribute of an award object.
    """
    filter_map = {
        'awarding_fpds': 'awarding_agency__fpds_code',
        'funding_fpds': 'funding_agency__fpds_code',
    }
    serializer_class = AwardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class AwardRetrieveViewSet(FilterQuerysetMixin, DetailViewSet):
    """
    DEPRECATED
    ## Spending data by Award (i.e. a grant, contract, loan, etc)
    This endpoint allows you to search and filter by almost any attribute of an award object.
    """
    filter_map = {
        'awarding_fpds': 'awarding_agency__fpds_code',
        'funding_fpds': 'funding_agency__fpds_code',
    }
    serializer_class = AwardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class SubawardAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    Return aggregated award information.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = Subaward.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name='post')
class SubawardAutocomplete(FilterQuerysetMixin, AutocompleteView):
    """
    DEPRECATED
    Autocomplete support for subaward objects.
    """
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    serializer_class = SubawardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Subaward.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class SubawardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    ## Spending data by Subaward
    This endpoint allows you to search and filter by almost any attribute of a subaward object.
    """
    serializer_class = SubawardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Subaward.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class SubawardRetrieveViewSet(FilterQuerysetMixin, DetailViewSet):
    """
    DEPRECATED
    ## Spending data by Subaward
    This endpoint allows you to search and filter by almost any attribute of a subaward object.
    """
    serializer_class = SubawardSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = Subaward.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class TransactionAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    Return aggregated transaction information.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = TransactionNormalized.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class TransactionListViewset(FilterQuerysetMixin, CachedDetailViewSet):
    """
    DEPRECATED
    Handles requests for award transaction data.
    """
    serializer_class = TransactionNormalizedSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = TransactionNormalized.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


@method_decorator(deprecated, name='list')
@method_decorator(deprecated, name='retrieve')
class TransactionRetrieveViewset(FilterQuerysetMixin, DetailViewSet):
    """
    DEPRECATED
    Handles requests for award transaction data.
    """
    serializer_class = TransactionNormalizedSerializer

    def get_queryset(self):
        """
        Return the view's queryset.
        """
        queryset = TransactionNormalized.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
