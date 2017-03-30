from collections import OrderedDict
from django.db.models import Avg, Count, F, Q, Max, Min, Sum, Func, IntegerField, ExpressionWrapper
from django.db.models.functions import ExtractDay, ExtractMonth, ExtractYear
from django.core.serializers.json import json, DjangoJSONEncoder
from django.utils.timezone import now

from usaspending_api.common.api_request_utils import FilterGenerator, ResponsePaginator, AutoCompleteHandler
from usaspending_api.common.exceptions import InvalidParameterException
from rest_framework_tracking.mixins import LoggingMixin

import logging


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

    _sql_function_transformations = {'fy': IntegerField}

    def _wrapped_f_expression(self, col_name):
        """F-expression of col, wrapped if needed with SQL function call

        Assumes that there's an SQL function defined for each
        registered lookup."""
        for suffix in self._sql_function_transformations:
            full_suffix = '__' + suffix
            if col_name.endswith(full_suffix):
                col_name = col_name[:-(len(full_suffix))]
                result = Func(F(col_name), function=suffix)
                output_type = self._sql_function_transformations[suffix]
                result = ExpressionWrapper(result, output_field=output_type())
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
        filters = None
        filter_map = kwargs.get('filter_map', {})
        fg = FilterGenerator(queryset.model, filter_map=filter_map)

        if len(request.data):
            fg = FilterGenerator(queryset.model)
            filters = fg.create_from_request_body(request.data)
        else:
            filters = Q(**fg.create_from_query_params(request.query_params))

        # Handle FTS vectors
        if len(fg.search_vectors) > 0:
            vector_sum = fg.search_vectors[0]
            for vector in fg.search_vectors[1:]:
                vector_sum += vector
            queryset = queryset.annotate(search=vector_sum)

        subwhere = filters
        # Create structure the query so we don't need to use distinct
        # This happens by reforming the request as 'WHERE pk_id IN (SELECT pk_id FROM queryset WHERE filters)'
        if len(filters) > 0:
            subwhere = Q(**{queryset.model._meta.pk.name + "__in": queryset.filter(filters).values_list(queryset.model._meta.pk.name, flat=True)})

        return queryset.filter(subwhere)

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


class AutocompleteResponseMixin(object):
    """Handles autocomplete responses and requests"""

    def build_response(self, request, *args, **kwargs):
        queryset = kwargs.get('queryset')

        serializer = kwargs.get('serializer')

        params = self.request.query_params.copy()  # copy() creates mutable copy of a QueryDict
        params.update(self.request.data.copy())

        return AutoCompleteHandler.handle(queryset, params, serializer)


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

        # get paged data for this request
        paged_data = ResponsePaginator.get_paged_data(
            queryset, request_parameters=params)

        # construct metadata of entire set of data that matches the request specifications
        total_metadata = {"count": paged_data.paginator.count}

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


class SuperLoggingMixin(LoggingMixin):

    events_logger = logging.getLogger("events")

    """Mixin to log requests - customized to disable DB logging, remove this method to re-enable"""
    def initial(self, request, *args, **kwargs):
        """Set current time on request"""

        # check if request method is being logged
        if self.logging_methods != '__all__' and request.method not in self.logging_methods:
            super(LoggingMixin, self).initial(request, *args, **kwargs)
            return None

        # get IP
        ipaddr = request.META.get("HTTP_X_FORWARDED_FOR", None)
        if ipaddr:
            # X_FORWARDED_FOR returns client1, proxy1, proxy2,...
            ipaddr = [x.strip() for x in ipaddr.split(",")][0]
        else:
            ipaddr = request.META.get("REMOTE_ADDR", "")

        # get view
        view_name = ''
        try:
            method = request.method.lower()
            attributes = getattr(self, method)
            view_name = (type(attributes.__self__).__module__ + '.' +
                         type(attributes.__self__).__name__)
        except Exception:
            pass

        # get the method of the view
        if hasattr(self, 'action'):
            view_method = self.action if self.action else ''
        else:
            view_method = method.lower()

        # save to log (as a dict, instead of to the db)
        self.request.log = {
            "requested_at": now(),
            "path": request.path,
            "view": view_name,
            "view_method": view_method,
            "remote_addr": ipaddr,
            "host": request.get_host(),
            "method": request.method,
            "query_params": request.query_params.dict(),
        }

        # regular initial, including auth check
        super(LoggingMixin, self).initial(request, *args, **kwargs)

        # add user to log after auth
        user = request.user
        if user.is_anonymous():
            user = None
        self.request.log["user"] = user

        # get data dict
        try:
            # Accessing request.data *for the first time* parses the request body, which may raise
            # ParseError and UnsupportedMediaType exceptions. It's important not to swallow these,
            # as (depending on implementation details) they may only get raised this once, and
            # DRF logic needs them to be raised by the view for error handling to work correctly.
            self.request.log["data"] = self.request.data.dict()
        except AttributeError:  # if already a dict, can't dictify
            self.request.log["data"] = self.request.data

    def finalize_response(self, request, response, *args, **kwargs):
        response = super(LoggingMixin, self).finalize_response(request, response, *args, **kwargs)

        # check if request method is being logged
        if self.logging_methods != '__all__' and request.method not in self.logging_methods:
            return response

        # compute response time
        response_timedelta = now() - self.request.log["requested_at"]
        response_ms = int(response_timedelta.total_seconds() * 1000)

        self.request.log["status_code"] = response.status_code
        self.request.log["response_ms"] = response_ms

        self.events_logger.info(self.request.log)
        # Return response
        return response
