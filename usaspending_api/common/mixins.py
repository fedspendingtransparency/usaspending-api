from django.db.models import Avg, Count, F, Max, Min, Sum
from django.db.models.functions import ExtractDay, ExtractMonth, ExtractYear
from rest_framework.response import Response

from usaspending_api.common.api_request_utils import FilterGenerator, FiscalYear


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
            serializer = self.get_serializer(aggregate, many=True)
        elif group_field:
            # group queryset by a non-date field and aggregate
            aggregate = (
                queryset.annotate(item=F(group_field)).values('item').annotate(
                    aggregate=agg_function(agg_field)))
            serializer = self.get_serializer(aggregate, many=True)
        else:
            # aggregate the entire queryset (i.e., no group by)
            aggregate = queryset.aggregate(aggregate=agg_function(agg_field))
            serializer = self.get_serializer(aggregate)

        return Response(serializer.data)

    def validate_request(self, params):
        """Validate request parameters."""

        agg_field = params.get('field')
        group_field = params.get('group')
        date_part = params.get('date_part')
        model = self.get_queryset().model

        # field to aggregate is required
        if agg_field is None:
            raise ValueError(
                'Request is missing the name of the field to aggregate'
            )

        # make sure the field we're aggregating exists in the model
        if hasattr(model, agg_field) is False:
            raise ValueError(
                'Field {} not found in model {}. '
                'Please specify a valid field in the request.'.format(agg_field, model))

        # make sure the field we're aggregating on is numeric
        # (there is likely a better way to do this?)
        numeric_fields = ['BigIntegerField', 'DecimalField', 'FloatField', 'IntegerField',
                          'PositiveIntegerField', 'PositiveSmallIntegerField',
                          'SmallIntegerField', 'DurationField']
        if model._meta.get_field(agg_field).get_internal_type() not in numeric_fields:
            raise ValueError(
                'Aggregate field {} is not a numeric type (e.g., integer, decimal)'.format(agg_field)
            )

        # if a group by field is specified, make sure it exists in the model
        if group_field is not None and hasattr(model, group_field) is False:
            raise ValueError(
                'Group field {} not found in model {}. '
                'Please specify a valid group field in the request'.format(group_field, model))

        # if a groupby date part is specified, make sure the groupby field is
        # a date and the groupby value is year, quarter, or month
        if date_part is not None and group_field is not None:
            # if the request is asking to group by a date component, the field
            # we're grouping by must be a date-related field
            # (there is probably a better way to do this?)
            date_fields = ['DateField', 'DateTimeField']
            if model._meta.get_field(group_field).get_internal_type() not in date_fields:
                raise ValueError(
                    'Group by date part ({}) requested for a non-date group by ({})'.format(
                        date_part, group_field)
                )
            # date_part must be a supported date component
            supported_date_parts = ['year', 'month', 'quarter', 'day']
            date_part = date_part.lower()
            if date_part not in supported_date_parts:
                raise ValueError(
                    'Date part {} is unsupported. Supported date parts are {}'.format(
                        date_part, supported_date_parts)
                )

        return agg_field, group_field, date_part


class FilterQuerysetMixin(object):
    """Filter a queryset based on request parameters."""

    def filter_records(self, request, *args, **kwargs):
        queryset = kwargs.get('queryset')

        # use request parameters to filter the passed queryset
        # note: is there a better way to differentiate between
        # GET and POST requests? can we get to a place where we
        # don't need to know?
        fg = FilterGenerator()
        if len(request.query_params):
            filters = fg.create_from_get(request.query_params)
            # add fiscal year to filters if requested
            # todo: streamline the GET filter logic
            if request.query_params.get('fy'):
                fy = FiscalYear(request.query_params.get('fy'))
                fy_arguments = fy.get_filter_object('date_signed', as_dict=True)
                filters = {**filters, **fy_arguments}
            return queryset.filter(**filters)
        else:
            filters = fg.create_from_post(request.data)
            return queryset.filter(filters)
