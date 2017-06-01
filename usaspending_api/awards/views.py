from collections import namedtuple
import json

from rest_framework import status
from rest_framework.exceptions import ParseError
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.awards.models import Award, Transaction, Subaward
from usaspending_api.awards.serializers import AwardSerializer, AwardTypeAwardSpendingSerializer, \
    TransactionSerializer, SubawardSerializer
from usaspending_api.common.api_request_utils import AutoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, SuperLoggingMixin, AggregateQuerysetMixin
from usaspending_api.common.views import DetailViewSet, AutocompleteView
from usaspending_api.common.serializers import AggregateSerializer
from django.db.models import F, Sum
from django.db import Error

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


class AwardTypeAwardSpendingViewSet(DetailViewSet):
    """Return all award spending by award type for a given fiscal year and agency id"""

    serializer_class = AwardTypeAwardSpendingSerializer

    def get_queryset(self):
        # retrieve post request payload
        json_request = self.request.query_params

        # retrieve fiscal_year & agency_id from request
        fiscal_year = json_request.get('fiscal_year', None)
        agency_id = json_request.get('agency_id', None)

        # required query parameters were not provided
        if not (fiscal_year and agency_id):
            raise ParseError('Missing one or more required query parameters: fiscal_year, agency_id')

        try:
            queryset = Transaction.objects.all()
            # Filter based on fiscal year and agency id
            queryset = queryset.filter(fiscal_year=fiscal_year, awarding_agency=agency_id)
            # alias awards.category to be award_type
            queryset = queryset.annotate(award_type=F('award__category'))
            # sum obligations for each category type
            queryset = queryset.values('award_type').annotate(amount_awarded=Sum('federal_action_obligation'))
        except Error:
            raise Exception('An unknown error occurred during execution of data retrieval')

        return queryset


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


class RecipientAwardSpendingViewSet(DetailViewSet):
    pass


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


class SubawardViewSet(DetailViewSet):
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
