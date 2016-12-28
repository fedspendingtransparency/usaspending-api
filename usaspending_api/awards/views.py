from collections import namedtuple
import json

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwardsTransactionObligations, Award
from usaspending_api.awards.serializers import (
    FinancialAccountsByAwardsTransactionObligationsSerializer, AwardSerializer)
from usaspending_api.common.api_request_utils import AutoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import AggregateView, DetailViewSet

AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


class AwardListViewSet(FilterQuerysetMixin,
                       ResponseMetadatasetMixin,
                       DetailViewSet):
    """Handles requests for award-level financial data."""

    serializer_class = FinancialAccountsByAwardsTransactionObligationsSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = FinancialAccountsByAwardsTransactionObligations.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class AwardListSummaryAutocomplete(APIView):
    """Autocomplete support for award summary objects."""
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            return Response(AutoCompleteHandler.handle(Award.objects.all(), body))
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AwardListAggregate(FilterQuerysetMixin,
                         AggregateView):
    """Return aggregate-level awards."""
    def get_queryset(self):
        queryset = FinancialAccountsByAwardsTransactionObligations.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class AwardListSummaryViewSet(FilterQuerysetMixin,
                              ResponseMetadatasetMixin,
                              DetailViewSet):
    """Handles requests for summarized award data."""

    filter_map = {
        'awarding_fpds': 'awarding_agency__fpds_code',
        'funding_fpds': 'funding_agency__fpds_code',
    }

    serializer_class = AwardSerializer

    def get_queryset(self):
        """Return the view's queryset."""
        queryset = Award.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
