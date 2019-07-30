from django.db.models import Avg, Count, F, Q, Max, Min, Sum, Func, IntegerField, ExpressionWrapper
from django.db.models.functions import ExtractDay, ExtractMonth, ExtractYear
from django.utils.timezone import now

from usaspending_api.common.api_request_utils import FilterGenerator, AutoCompleteHandler
from usaspending_api.common.exceptions import InvalidParameterException
from rest_framework_tracking.mixins import LoggingMixin

import logging


class AggregateQuerysetMixin(object):
    """
    Aggregate a queryset.
    Any pre-aggregation operations on the queryset (e.g. filtering) is already done (it's handled at the view level,
    in the get_queryset method).
    """

    def aggregate(self, request, *args, **kwargs):
        """Perform an aggregate function on a Django queryset with an optional group by field."""
        # create a single dict that contains the requested aggregate parameters, regardless of request type
        # (e.g., GET, POST) (not sure if this is a good practice, or we should be more prescriptive that aggregate
        # requests can only be of one type)
        params = dict(request.query_params)
        params.update(dict(request.data))

        # get the queryset to be aggregated
        queryset = kwargs.get("queryset", None)

        # validate request parameters
        agg_field, group_fields, date_part = self.validate_request(params, queryset)

        # Check for null opt-in, and filter instances where all group fields are null
        if not params.get("show_null_groups", False) and not params.get("show_nulls", False):
            q_object = Q()
            for field in group_fields:
                q_object = q_object | Q(**{"{}__isnull".format(field): False})
            queryset = queryset.filter(q_object)

        # Check for null opt-in, and filter instances where the aggregate field is null
        if not params.get("show_null_aggregates", False) and not params.get("show_nulls", False):
            q_object = Q(**{"{}__isnull".format(agg_field): False})
            queryset = queryset.filter(q_object)

        # get the aggregate function to use (default is Sum)
        agg_map = {"avg": Avg, "count": Count, "min": Min, "max": Max, "sum": Sum}
        agg_function = params.get("aggregate", "sum").lower()
        agg_function = agg_map.get(agg_function, Sum)

        if group_fields and date_part:
            # group queryset by a date field and aggregate
            group_func_map = {"year": ExtractYear, "month": ExtractMonth, "day": ExtractDay}
            group_func = group_func_map.get(date_part)
            aggregate = (
                queryset.annotate(item=group_func(group_fields[0]))
                .values("item")
                .annotate(aggregate=agg_function(agg_field))
            )
        else:
            # item is deprecated and should be removed soon group queryset by a non-date field and aggregate

            # Support expression wrappers on all items in the group field array
            # We must do this so users can specify a __fy request on any field
            # in any order, rather than being required to do so as the first item
            item_annotations = {"item": self._wrapped_f_expression(group_fields[0])}
            for gf in group_fields:
                expr = self._wrapped_f_expression(gf)
                if isinstance(expr, ExpressionWrapper):
                    item_annotations[gf] = expr
            group_fields.append("item")
            aggregate = (
                queryset.annotate(**item_annotations).values(*group_fields).annotate(aggregate=agg_function(agg_field))
            )

        return aggregate

    _sql_function_transformations = {"fy": IntegerField}

    def _wrapped_f_expression(self, col_name):
        """F-expression of col, wrapped if needed with SQL function call

        Assumes that there's an SQL function defined for each registered lookup.
        """
        for suffix in self._sql_function_transformations:
            full_suffix = "__" + suffix
            if col_name.endswith(full_suffix):
                col_name = col_name[: -(len(full_suffix))]
                result = Func(F(col_name), function=suffix)
                output_type = self._sql_function_transformations[suffix]
                result = ExpressionWrapper(result, output_field=output_type())
                return result
        return F(col_name)

    def validate_request(self, params, queryset):
        """Validate request parameters."""

        agg_field = params.get("field")
        group_fields = params.get("group")
        date_part = params.get("date_part")
        model = queryset.model

        # field to aggregate is required
        if agg_field is None:
            raise InvalidParameterException("Request is missing the name of the field to aggregate")

        # make sure the field we're aggregating exists in the model
        if hasattr(model, agg_field) is False:
            raise InvalidParameterException(
                "Field {} not found in model {}. "
                "Please specify a valid field in the request.".format(agg_field, model)
            )

        # make sure the field we're aggregating on is numeric
        # (there is likely a better way to do this?)
        numeric_fields = [
            "BigIntegerField",
            "DecimalField",
            "FloatField",
            "IntegerField",
            "PositiveIntegerField",
            "PositiveSmallIntegerField",
            "SmallIntegerField",
            "DurationField",
        ]
        if model._meta.get_field(agg_field).get_internal_type() not in numeric_fields:
            raise InvalidParameterException(
                "Aggregate field {} is not a numeric type (e.g., integer, decimal)".format(agg_field)
            )

        # field to group by is required
        if group_fields is None:
            raise InvalidParameterException("Request is missing the field to group by")

        # make sure group fields is a list
        if not isinstance(group_fields, list):
            group_fields = [group_fields]

        # if a groupby date part is specified, make sure the groupby field is
        # a date and the groupby value is year, quarter, or month
        if date_part is not None:
            # only allow date parts when grouping by a single field (for now)
            if len(group_fields) > 1:
                raise InvalidParameterException("Date parts are only valid when grouping by a single field.")
            # if the request is asking to group by a date component, the field
            # we're grouping by must be a date-related field (there is probably a better way to do this?)
            date_fields = ["DateField", "DateTimeField"]
            if model._meta.get_field(group_fields[0]).get_internal_type() not in date_fields:
                raise InvalidParameterException(
                    "Group by date part ({}) requested for a non-date group by ({})".format(date_part, group_fields[0])
                )
            # date_part must be a supported date component
            supported_date_parts = ["year", "month", "quarter", "day"]
            date_part = date_part.lower()
            if date_part not in supported_date_parts:
                raise InvalidParameterException(
                    "Date part {} is unsupported. Supported date parts are {}".format(date_part, supported_date_parts)
                )

        return agg_field, group_fields, date_part


class FilterQuerysetMixin(object):
    """Handles queryset filtering."""

    def filter_records(self, request, *args, **kwargs):
        """Filter a queryset based on request parameters"""
        queryset = kwargs.get("queryset")

        # If there is data in the request body, use that to create filters. Otherwise, use information in the request's
        # query params to create filters. Eventually, we should refactor the filter creation process to accept a list
        # of paramaters and create filters without needing to know about the structure of the request itself.
        filters = None
        filter_map = kwargs.get("filter_map", {})
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
            subwhere = Q(
                **{
                    queryset.model._meta.pk.name
                    + "__in": queryset.filter(filters).values_list(queryset.model._meta.pk.name, flat=True)
                }
            )

        return queryset.filter(subwhere)

    def order_records(self, request, *args, **kwargs):
        """Order a queryset based on request parameters."""
        queryset = kwargs.get("queryset")

        # create a single dict that contains the requested aggregate parameters,
        # regardless of request type (e.g., GET, POST)
        # (not sure if this is a good practice, or we should be more
        # prescriptive that aggregate requests can only be of one type)

        params = dict(request.query_params)
        params.update(dict(request.data))
        ordering = params.get("order")
        if ordering is not None:
            return queryset.order_by(*ordering)
        else:
            return queryset


class AutocompleteResponseMixin(object):
    """Handles autocomplete responses and requests"""

    def build_response(self, request, *args, **kwargs):
        queryset = kwargs.get("queryset")

        serializer = kwargs.get("serializer")

        params = request.query_params.copy()  # copy() creates mutable copy of a QueryDict
        params.update(request.data.copy())

        return AutoCompleteHandler.handle(queryset, params, serializer)


class SuperLoggingMixin(LoggingMixin):

    events_logger = logging.getLogger("events")

    """Mixin to log requests - customized to disable DB logging, remove this method to re-enable"""

    def initial(self, request, *args, **kwargs):
        """Set current time on request"""

        # check if request method is being logged
        if self.logging_methods != "__all__" and request.method not in self.logging_methods:
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
        view_name = ""
        try:
            method = request.method.lower()
            attributes = getattr(self, method)
            view_name = type(attributes.__self__).__module__ + "." + type(attributes.__self__).__name__
        except Exception:
            pass

        # get the method of the view
        if hasattr(self, "action"):
            view_method = self.action if self.action else ""
        else:
            view_method = method.lower()

        # save to log (as a dict, instead of to the db)
        request.log = {
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
        request.log["user"] = user

        # get data dict
        try:
            # Accessing request.data *for the first time* parses the request body, which may raise
            # ParseError and UnsupportedMediaType exceptions. It's important not to swallow these,
            # as (depending on implementation details) they may only get raised this once, and
            # DRF logic needs them to be raised by the view for error handling to work correctly.
            request.log["data"] = request.data.dict()
        except AttributeError:  # if already a dict, can't dictify
            request.log["data"] = request.data

    def finalize_response(self, request, response, *args, **kwargs):
        response = super(LoggingMixin, self).finalize_response(request, response, *args, **kwargs)

        # check if request method is being logged
        if self.logging_methods != "__all__" and request.method not in self.logging_methods:
            return response

        # compute response time
        response_timedelta = now() - request.log["requested_at"]
        response_ms = int(response_timedelta.total_seconds() * 1000)

        request.log["status_code"] = response.status_code
        request.log["response_ms"] = response_ms

        self.events_logger.info(request.log)
        # Return response
        return response
