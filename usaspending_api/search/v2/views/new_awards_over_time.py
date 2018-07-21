import logging

from django.conf import settings
from django.contrib.postgres.aggregates.general import ArrayAgg
from django.db.models import Func, IntegerField
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.settings import API_MAX_DATE, API_SEARCH_MIN_DATE

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


class Month(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from %(expressions)s)"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) - INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) - INTERVAL '3 months')"
    output_field = IntegerField()


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class NewAwardsOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    endpoint_doc: /advanced_award_search/spending_over_time.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        valid_groups = ['quarter', 'fiscal_year', 'month', 'fy', 'q', 'm']
        models = [
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False},
            {'name': 'group', 'key': 'group', 'type': 'enum', 'enum_values': valid_groups, 'optional': True, 'default': 'fiscal_year'},
            {'name': 'recipient_id', 'key': 'filters|recipient_id', 'type': 'text', 'text_type': 'search', 'optional': False},
            {'name': 'time_period', 'key': 'filters|time_period', 'type': 'array', 'array_type': 'object', 'optional': False, 'object_keys': {
                'start_date': {'type': 'date', 'min': settings.API_SEARCH_MIN_DATE, 'max': settings.API_MAX_DATE},
                'end_date': {'type': 'date', 'min': settings.API_SEARCH_MIN_DATE, 'max': settings.API_MAX_DATE},
                'date_type': {'type': 'enum', 'enum_values': ['action_date', 'last_modified_date'], 'optional': True,
                      'default': 'action_date'}
            }},
        ]
        # models.extend(copy.deepcopy(AWARD_FILTER))
        json_request = TinyShield(models).block(request.data)
        group = json_request['group']
        subawards = json_request['subawards']
        filters = json_request.get("filters", None)

        if filters is None:
            raise InvalidParameterException('Missing request parameters: filters')

        if subawards:
            raise NotImplementedError('subawards are not implemented yet')

        queryset = combine_date_range_queryset(
            json_request['filters']['time_period'],
            UniversalTransactionView,
            API_SEARCH_MIN_DATE,
            API_MAX_DATE
        )
        queryset = queryset.filter(recipient_hash=json_request['filters']['recipient_id'][:-2])

        values = ['year']
        if group in ('m', 'month'):
            queryset = queryset.annotate(
                month=Month('action_date'),
                year=FiscalYear('action_date'))
            values.append('month')
        elif group in ('q', 'quarter'):
            queryset = queryset.annotate(
                quarter=FiscalQuarter('action_date'),
                year=FiscalYear('action_date'))
            values.append('quarter')
        elif group in ('fy', 'fiscal_year'):
            queryset = queryset.annotate(year=FiscalYear('action_date'))

        queryset = queryset \
            .values(*values) \
            .annotate(award_ids=ArrayAgg('award_id')) \
            .order_by(*['-{}'.format(value) for value in values])

        from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
        print('=======================================')
        print(request.path)
        print(generate_raw_quoted_query(queryset))
        results = []
        previous_awards = set()
        for row in queryset:
            new_awards = set(row['award_ids']) - previous_awards
            result = {
                "time_period": {},
                "award_ids": set(row['award_ids']),  # TEMPORARY
                "transactions_in_period": len(row['award_ids']),  # TEMPORARY
                "new_award_count_in_period": len(new_awards),
            }
            for period in values:
                result["time_period"]["fiscal_{}".format(period)] = row[period]
            results.append(result)
            previous_awards.update(row['award_ids'])
        return Response(results)

        # build response
        # "results": [
        # {
        #     "time_period": {
        #         "fiscal_year": "2017",
        #         "quarter": 1
        #     },
        #     "new_awards": 447434495.76
        # },
