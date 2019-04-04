from collections import namedtuple
from django_filters import rest_framework as filters
from rest_framework import viewsets

from usaspending_api.awards.models import Award, Subaward, FinancialAccountsByAwards
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.serializers import AwardSerializer, SubawardSerializer, TransactionNormalizedSerializer, FinancialAccountsByAwardsSerializer
from usaspending_api.common.mixins import FilterQuerysetMixin, AggregateQuerysetMixin
from usaspending_api.common.serializers import AggregateSerializer
from usaspending_api.common.views import DetailViewSet, CachedDetailViewSet, AutocompleteView

AggregateItem = namedtuple('AggregateItem', ['field', 'func'])


class AwardAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = Award.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class AwardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
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
        """
        Return the view's queryset.
        """
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class AwardRetrieveViewSet(FilterQuerysetMixin, DetailViewSet):
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
        """
        Return the view's queryset.
        """
        queryset = Award.nonempty.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)
        filtered_queryset = self.filter_records(self.request, queryset=queryset, filter_map=self.filter_map)
        ordered_queryset = self.order_records(self.request, queryset=filtered_queryset)
        return ordered_queryset


class SubawardAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated award information.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = Subaward.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class SubawardAutocomplete(FilterQuerysetMixin, AutocompleteView):
    """
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


class SubawardListViewSet(FilterQuerysetMixin, CachedDetailViewSet):
    """
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


class SubawardRetrieveViewSet(FilterQuerysetMixin, DetailViewSet):
    """
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


class TransactionAggregateViewSet(FilterQuerysetMixin, AggregateQuerysetMixin, CachedDetailViewSet):
    """
    Return aggregated transaction information.
    """
    serializer_class = AggregateSerializer

    def get_queryset(self):
        queryset = TransactionNormalized.objects.all()
        queryset = self.filter_records(self.request, queryset=queryset)
        queryset = self.aggregate(self.request, queryset=queryset)
        queryset = self.order_records(self.request, queryset=queryset)
        return queryset


class TransactionListViewset(FilterQuerysetMixin, CachedDetailViewSet):
    """
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


class TransactionRetrieveViewset(FilterQuerysetMixin, DetailViewSet):
    """
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

class FABAFilter(filters.FilterSet):

    class Meta:
        model = FinancialAccountsByAwards
        # You can create any relationships you want here using ORM notation
        fields = ('piid', 'treasury_account__agency_id')

class FinancialAccountsByAwardsListViewset(viewsets.ReadOnlyModelViewSet):

    '''
        I spent a lot of time investigating the serializers here trying to come up with a better solution. However
        ultimately I determined that the LimitableSerializer class which we base all our serializers on is actually
        fairly well put together. It handles prefetching well, so that the nested serializers don't prompt new queries,
        and it doesn't really mess with the core functionality of the serializer as written in DRF. My main objection 
        to them is having to specify default_fields unless verbose is set. If I was ultimately going to rewrite this,
        I might switch that around so that verbose is the default. I did not feel that that was worth investing time in
        for this investigation however.
    '''

    serializer_class = FinancialAccountsByAwardsSerializer

    # filterset_class is a reserved field that DRF looks for to generate the filtering architecture
    filterset_class = FABAFilter

    def filter_queryset(self, queryset):

        # This method is an override that is necessary for using DjangoFilterBackend with DRF viewsets

        filter_backends = (filters.DjangoFilterBackend, )

        for backend in list(filter_backends):
            queryset = backend().filter_queryset(self.request, queryset, view=self)

        return queryset


    def get_queryset(self):

        queryset = FinancialAccountsByAwards.objects.all()
        queryset = self.serializer_class.setup_eager_loading(queryset)

        return queryset

    def list(self, request, *args, **kwargs):
        try:
            # Get the queryset (this will handle filtering and ordering)
            queryset = self.filter_queryset(self.get_queryset())
            # Grab the page of data
            page = self.paginate_queryset(queryset)
            # Serialize the page
            serializer = self.get_serializer(page, read_only=True, many=True)
            # Return the paginated response
            return self.get_paginated_response(serializer.data)
        except InvalidParameterException as e:
            response = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            self.exception_logger.exception(e)
            return Response(response, status=status_code)
        except Exception as e:
            response = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            self.exception_logger.exception(e)
            return Response(response, status=status_code)

