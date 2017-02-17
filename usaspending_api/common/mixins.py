from collections import OrderedDict
from itertools import chain
from django.db.models import Avg, Count, F, Max, Min, Sum, Func, DateField
from django.db.models.functions import ExtractDay, ExtractMonth, ExtractYear

from usaspending_api.common.api_request_utils import FilterGenerator, FiscalYear, ResponsePaginator
from usaspending_api.common.exceptions import InvalidParameterException


class AggregateQuerysetMixin(object):
    """
    Aggregate a queryset.
    Any pre-aggregation operations on the queryset (e.g. filtering)
    is already done (it's handled at the view level, in the
    get_queryset method).
    """

    def aggregate(self, request, *args, **kwargs):
        """Perform an aggregate function on a Django queryset with an optional group by field."""
        # create a single dict that contains the requested aggregate parameters,
        # regardless of request type (e.g., GET, POST)
        # (not sure if this is a good practice, or we should be more
        # prescriptive that aggregate requests can only be of one type)
        params = dict(request.query_params)
        params.update(dict(request.data))

        # validate request parameters
        agg_field, group_field, date_part = self.validate_request(params)

        # get the queryset to be aggregated
        queryset = self.get_queryset()

        # get the aggregate function to use (default is Sum)
        agg_map = {
            'avg': Avg,
            'count': Count,
            'min': Min,
            'max': Max,
            'sum': Sum
        }
        agg_function = params.get('aggregate', 'sum').lower()
        agg_function = agg_map.get(agg_function, Sum)

        if group_field and date_part:
            # group queryset by a date field and aggregate
            group_func_map = {
                'year': ExtractYear,
                'month': ExtractMonth,
                'day': ExtractDay
            }
            group_func = group_func_map.get(date_part)
            aggregate = (
                queryset.annotate(item=group_func(group_field)).values('item').annotate(
                    aggregate=agg_function(agg_field)))
        else:
            group_expr = self._wrapped_f_expression(group_field)
            # group queryset by a non-date field and aggregate
            aggregate = (
                queryset.annotate(item=group_expr).values('item').annotate(
                    aggregate=agg_function(agg_field)))

        return aggregate

    classes_with_lookups = (DateField, )

    lookup_suffixes = set()
    for cls in (DateField, ):
        for suffix in cls.class_lookups.keys():
            lookup_suffixes.add(suffix)

    def _wrapped_f_expression(self, col_name):
        """F-expression of col, wrapped if needed with SQL function call

        Assumes that there's an SQL function defined for each
        registered lookup."""
        for suffix in self.lookup_suffixes:
            full_suffix = '__' + suffix
            if col_name.endswith(full_suffix):
                col_name = col_name[:-(len(full_suffix))]
                result = Func(F(col_name), function=suffix)
                return result
        return F(col_name)


    def validate_request(self, params):
        """Validate request parameters."""

        agg_field = params.get('field')
        group_field = params.get('group')
        date_part = params.get('date_part')
        model = self.get_queryset().model

        # field to aggregate is required
        if agg_field is None:
            raise InvalidParameterException(
                'Request is missing the name of the field to aggregate'
            )

        # make sure the field we're aggregating exists in the model
        if hasattr(model, agg_field) is False:
            raise InvalidParameterException(
                'Field {} not found in model {}. '
                'Please specify a valid field in the request.'.format(agg_field, model))

        # make sure the field we're aggregating on is numeric
        # (there is likely a better way to do this?)
        numeric_fields = ['BigIntegerField', 'DecimalField', 'FloatField', 'IntegerField',
                          'PositiveIntegerField', 'PositiveSmallIntegerField',
                          'SmallIntegerField', 'DurationField']
        if model._meta.get_field(agg_field).get_internal_type() not in numeric_fields:
            raise InvalidParameterException(
                'Aggregate field {} is not a numeric type (e.g., integer, decimal)'.format(agg_field)
            )

        # field to group by is required
        if group_field is None:
            raise InvalidParameterException(
                'Request is missing the field to group by'
            )

        # if a groupby date part is specified, make sure the groupby field is
        # a date and the groupby value is year, quarter, or month
        if date_part is not None:
            # if the request is asking to group by a date component, the field
            # we're grouping by must be a date-related field
            # (there is probably a better way to do this?)
            date_fields = ['DateField', 'DateTimeField']
            if model._meta.get_field(group_field).get_internal_type() not in date_fields:
                raise InvalidParameterException(
                    'Group by date part ({}) requested for a non-date group by ({})'.format(
                        date_part, group_field)
                )
            # date_part must be a supported date component
            supported_date_parts = ['year', 'month', 'quarter', 'day']
            date_part = date_part.lower()
            if date_part not in supported_date_parts:
                raise InvalidParameterException(
                    'Date part {} is unsupported. Supported date parts are {}'.format(
                        date_part, supported_date_parts)
                )

        return agg_field, group_field, date_part


class FilterQuerysetMixin(object):
    """Handles queryset filtering."""

    def filter_records(self, request, *args, **kwargs):
        """Filter a queryset based on request parameters"""
        queryset = kwargs.get('queryset')

        # If there is data in the request body, use that
        # to create filters. Otherwise, use information
        # in the request's query params to create filters.
        # Eventually, we should refactor the filter creation
        # process to accept a list of paramaters
        # and create filters without needing to know about the structure
        # of the request itself.
        if len(request.data):
            fg = FilterGenerator()
            filters = fg.create_from_request_body(request.data)
            return queryset.filter(filters).distinct()
        else:
            filter_map = kwargs.get('filter_map', {})
            fg = FilterGenerator(filter_map=filter_map)
            filters = fg.create_from_query_params(request.query_params)
            # add fiscal year to filters if requested
            # deprecated: we plan to start storing fiscal years in the database
            if request.query_params.get('fy'):
                fy = FiscalYear(request.query_params.get('fy'))
                fy_arguments = fy.get_filter_object('date_signed', as_dict=True)
                filters = {**filters, **fy_arguments}
            return queryset.filter(**filters).distinct()

    def order_records(self, request, *args, **kwargs):
        """Order a queryset based on request parameters."""
        queryset = kwargs.get('queryset')

        # create a single dict that contains the requested aggregate parameters,
        # regardless of request type (e.g., GET, POST)
        # (not sure if this is a good practice, or we should be more
        # prescriptive that aggregate requests can only be of one type)
        params = dict(request.query_params)
        params.update(dict(request.data))
        ordering = params.get('order')
        if ordering is not None:
            return queryset.order_by(*ordering)
        else:
            return queryset


class ResponseMetadatasetMixin(object):
    """Handles response metadata."""

    # This mixin ensures that views which have been
    # refactored to use generic views/viewsets and mixins
    # send back metadata consistent with the views that
    # haven't yet been updated. Going forward, we can
    # probably handle metadata in way that's more consistent
    # with Django Rest Framework constructs (e.g., using
    # the pagination that comes for free in generic views)

    def build_response(self, request, *args, **kwargs):
        """Returns total and page metadata that can be attached to a response."""
        queryset = kwargs.get('queryset')

        # workaround to handle both GET and POST requests
        params = self.request.query_params.copy()  # copy() creates mutable copy of a QueryDict
        params.update(self.request.data.copy())

        # construct metadata of entire set of data that matches the request specifications
        total_metadata = {"count": queryset.count()}

        # get paged data for this request
        paged_data = ResponsePaginator.get_paged_data(
            queryset, request_parameters=params)

        # construct page-specific metadata
        page_metadata = {
            "page_number": paged_data.number,
            "num_pages": paged_data.paginator.num_pages,
            "count": len(paged_data)
        }

        # note that generics/viewsets pass request and view info to the
        # serializer context automatically. however, we explicitly add it here
        # because our common DetailViewSet overrides the 'list' method, which
        # somehow prevents the extra info from being added to the serializer
        # context. because we can get rid of DetailViewSet and use
        # ReadOnlyModelViewSet directly as soon as the pagination changes
        # are in, not going spend a lot of time researching this.
        context = {'request': request, 'view': self}
        # serialize the paged data
        serializer = kwargs.get('serializer')(paged_data, many=True, context=context)
        serialized_data = serializer.data

        response_object = OrderedDict({
            "total_metadata": total_metadata,
            "page_metadata": page_metadata
        })
        response_object.update({'results': serialized_data})

        return response_object
