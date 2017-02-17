from collections import namedtuple
import json

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from usaspending_api.awards.models import Award, Transaction
from usaspending_api.awards.serializers import AwardSerializer, TransactionSerializer
from usaspending_api.common.api_request_utils import AutoCompleteHandler
from usaspending_api.common.mixins import FilterQuerysetMixin, ResponseMetadatasetMixin
from usaspending_api.common.views import AggregateView, DetailViewSet

AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


class AwardViewSet(FilterQuerysetMixin,
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


class AwardAutocomplete(APIView):
    """Autocomplete support for award summary objects."""
    # Maybe refactor this out into a nifty autocomplete abstract class we can just inherit?
    def post(self, request, format=None):
        try:
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            return Response(AutoCompleteHandler.handle(Award.objects.all(), body, AwardSerializer))
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AwardAggregateViewSet(FilterQuerysetMixin,
                            AggregateView):
    """Return aggregated award information."""
    def get_queryset(self):
        queryset = Award.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class TransactionViewset(FilterQuerysetMixin,
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


class TransactionAggregateViewSet(FilterQuerysetMixin,
                                  AggregateView):
    """Return aggregated transaction information."""
    def get_queryset(self):
        queryset = Transaction.objects.all()
        filtered_queryset = self.filter_records(self.request, queryset=queryset)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset
