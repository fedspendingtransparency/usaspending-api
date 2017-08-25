from collections import namedtuple

from usaspending_api.awards.models import Award, Transaction, Subaward
from usaspending_api.awards.serializers import AwardSerializer, SubawardSerializer, TransactionSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin, AggregateQuerysetMixin
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.views import DetailViewSet, AutocompleteView

AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


class AwardAggregateViewSet(SuperLoggingMixin,
                            FilterQuerysetMixin,
                            AggregateQuerysetMixin,
                            DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated award information."""
    def get_queryset(self):
        queryset = Award.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class AwardAutocomplete(FilterQuerysetMixin,
                        AutocompleteView):
    """Autocomplete support for award summary objects."""
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    serializer_class = AwardSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class AwardViewSet(SuperLoggingMixin,
                   FilterQuerysetMixin,
                   DetailViewSet):
    """
    ## Spending data by Award (i.e. a grant, contract, loan, etc)

    This endpoint allows you to search and filter by almost any attribute of an award object.

    """

    filter_map = {
        'awarding_fpds': 'awarding_agency__fpds_code',
        'funding_fpds': 'funding_agency__fpds_code',
    }

    serializer_class = AwardSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class SubawardAggregateViewSet(SuperLoggingMixin,
                               FilterQuerysetMixin,
                               AggregateQuerysetMixin,
                               DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated award information."""
    def get_queryset(self):
        queryset = Subaward.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class SubawardAutocomplete(FilterQuerysetMixin,
                           AutocompleteView):
    """Autocomplete support for subaward objects."""
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    serializer_class = SubawardSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Subaward.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class SubawardViewSet(FilterQuerysetMixin, DetailViewSet):
    """
    ## Spending data by Subaward

    This endpoint allows you to search and filter by almost any attribute of a subaward object.

    """

    serializer_class = SubawardSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Subaward.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TransactionAggregateViewSet(SuperLoggingMixin,
                                  FilterQuerysetMixin,
                                  AggregateQuerysetMixin,
                                  DetailViewSet):

    serializer_class = AggregateSerializer

    """Return aggregated transaction information."""
    def get_queryset(self):
        queryset = Transaction.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TransactionViewset(SuperLoggingMixin,
                         FilterQuerysetMixin,
                         DetailViewSet):
    """Handles requests for award transaction data."""
    serializer_class = TransactionSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Transaction.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
