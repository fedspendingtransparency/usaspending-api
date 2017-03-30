from collections import namedtuple
import json

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.awards.models import Award, Transaction
from usaspending_api.awards.serializers import AwardSerializer, TransactionSerializer
from usaspending_api.common.api_request_utils import AutoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin, SuperLoggingMixin
from usaspending_api.common.views import AggregateView, DetailViewSet, AutocompleteView

AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


class AwardViewSet(SuperLoggingMixin,
                   FilterQuerysetMixin,
                   ResponseMetadatasetMixin,
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
        return filtered_queryset


class AwardAggregateViewSet(SuperLoggingMixin,
                            FilterQuerysetMixin,
                            AggregateView):
    """Return aggregated award information."""
    def get_queryset(self):
        queryset = Award.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TransactionViewset(SuperLoggingMixin,
                         FilterQuerysetMixin,
                         ResponseMetadatasetMixin,
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


class TransactionAggregateViewSet(SuperLoggingMixin,
                                  FilterQuerysetMixin,
                                  AggregateView):
    """Return aggregated transaction information."""
    def get_queryset(self):
        queryset = Transaction.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
