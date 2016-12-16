from django.db.models import Avg, F, Max, Min, Sum
from django.db.models.functions import Trunc


class AggregateHandler():
    """Handles processing of aggregate endpoints."""

    def __init__(self, request_body, query):
        self.request_body = request_body
        self.query = query
        self.agg_function = request_body.get('aggregate')
        self.agg_field = self.request_body.get('field')
        self.group_field = self.request_body.get('group')
        self.date_part = self.request_body.get('date_part')

    def aggregate_query(self):
        """Perform an aggregate function on a Django queryset with an optional group by field."""

        self.validate_request()

        agg_function = self.agg_function
        if agg_function == 'avg':
            agg_function = Avg
        elif agg_function == 'min':
            agg_function = Min
        elif agg_function == 'max':
            agg_function = Max
        else:
            # if aggregation function not specified in request, or
            # if request specified anything other than avg, min, max,
            # or sum, default to sum
            agg_function = Sum

        agg_field = self.agg_field
        group_field = self.group_field
        date_part = self.date_part
        query = self.query

        if group_field and date_part:
            aggregate = (
                query.annotate(item=Trunc(group_field, date_part)).values('item').annotate(
                    aggregate=agg_function(agg_field)))
        elif group_field:
            aggregate = (
                query.annotate(item=F(group_field)).values('item').annotate(
                    aggregate=agg_function(agg_field)))
        else:
            # todo: this doesn't work b/c it returns a dictionary instead of a queryset
            aggregate = query.aggregate(aggregate=agg_function(agg_field))

        return aggregate

    def validate_request(self):
        """Validate request parameters."""

        # field to aggregate is required
        if self.agg_field is None:
            raise ValueError(
                'Request is missing the name of the field to aggregate'
            )

        # make sure the field we're aggregating exists in the model
        model = self.query.model
        if getattr(model, self.agg_field) is None:
            raise ValueError(
                'Field {} not found in model {}. '
                'Please specify a valid field in the request.'.format(self.agg_field, model))

        # make sure the field we're aggregating on is numeric

        # currently, we need a group by field (fix to allow no group by is on the way)
        if self.group_field is None:
            raise ValueError(
                'Request is missing the group parameter'
            )

        # if a group by field is specified, make sure it exists in the model
        if getattr(model, self.group_field) is None:
            raise ValueError(
                'Group field {} not found in model {}. '
                'Please specify a valid group field in the request'.format(self.group_field, model))

        # if a groupby date part is specified, make sure the groupby field is
        # a date and the groupby value is year, quarter, or month

        return True
