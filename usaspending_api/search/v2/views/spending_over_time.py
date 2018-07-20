import ast
import copy
import logging

from collections import OrderedDict
from datetime import date
from fiscalyear import FiscalDate

from django.conf import settings
from django.db.models import Sum
from django.db.models.functions import ExtractMonth
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import generate_fiscal_month
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield


logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingOverTimeVisualizationViewSet(APIView):
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
            {'name': 'group', 'key': 'group', 'type': 'enum', 'enum_values': valid_groups, 'optional': False}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)
        group = json_request['group']
        subawards = json_request['subawards']
        filters = json_request.get("filters", None)

        if filters is None:
            raise InvalidParameterException('Missing request parameters: filters')

        # define what values are needed in the sql query
        # we do not use matviews for Subaward filtering, just the Subaward download filters

        if subawards:
            queryset = subaward_filter(filters)
        else:
            queryset = spending_over_time(filters).values('action_date', 'generated_pragmatic_obligation')

        # build response
        response = {'group': group, 'results': []}
        nested_order = ''

        # list of time_period objects ie {"fy": "2017", "quarter": "3"} : 1000
        group_results = OrderedDict()

        # for Subawards we extract data from action_date
        if subawards:
            data_set = queryset \
                .values('award_type') \
                .annotate(month=ExtractMonth('action_date'), transaction_amount=Sum('amount')) \
                .values('month', 'fiscal_year', 'transaction_amount')
        else:
            # for Awards we Sum generated_pragmatic_obligation for transaction_amount
            queryset = queryset.values('fiscal_year')
            if group in ('fy', 'fiscal_year'):
                data_set = queryset \
                    .annotate(transaction_amount=Sum('generated_pragmatic_obligation')) \
                    .values('fiscal_year', 'transaction_amount')
            else:
                # quarterly also takes months and aggregates the data
                data_set = queryset \
                    .annotate(
                        month=ExtractMonth('action_date'),
                        transaction_amount=Sum('generated_pragmatic_obligation')) \
                    .values('fiscal_year', 'month', 'transaction_amount')

        for record in data_set:
            # generate unique key by fiscal date, depending on group
            key = {'fiscal_year': str(record['fiscal_year'])}
            if group in ('m', 'month'):
                # generate the fiscal month
                key['month'] = generate_fiscal_month(date(year=2017, day=1, month=record['month']))
                nested_order = 'month'
            elif group in ('q', 'quarter'):
                # generate the fiscal quarter
                key['quarter'] = FiscalDate(2017, record['month'], 1).quarter
                nested_order = 'quarter'
            key = str(key)

            # if key exists, aggregate
            if group_results.get(key) is None:
                group_results[key] = record['transaction_amount']
            else:
                group_results[key] = group_results.get(key) + record['transaction_amount']

        # convert result into expected format, sort by key to meet front-end specs
        results = []
        # Expected results structure
        # [{
        # 'time_period': {'fy': '2017', 'quarter': '3'},
        # 'aggregated_amount': '200000000'
        # }]
        sorted_group_results = sorted(
            group_results.items(),
            key=lambda k: (
                ast.literal_eval(k[0])['fiscal_year'],
                int(ast.literal_eval(k[0])[nested_order])) if nested_order else (ast.literal_eval(k[0])['fiscal_year']))

        for key, value in sorted_group_results:
            key_dict = ast.literal_eval(key)
            result = {'time_period': key_dict, 'aggregated_amount': float(value) if value else float(0)}
            results.append(result)
        response['results'] = results

        return Response(response)
