import copy
import logging

from django.conf import settings
from django.db.models import Count
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import bolster_missing_time_periods
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile, SummaryAwardRecipient
from usaspending_api.settings import API_MAX_DATE, API_SEARCH_MIN_DATE
from usaspending_api.common.helpers.sql_helpers import FiscalMonth, FiscalQuarter, FiscalYear

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


class NewAwardsOverTimeVisualizationViewSet(APIView):
    """
    endpoint_doc: /advanced_award_search/new_awards_over_time.md
    """

    def validate_api_request(self, json_payload):
        self.groupings = {
            'quarter': 'quarter',
            'q': 'quarter',
            'fiscal_year': 'fiscal_year',
            'fy': 'fiscal_year',
            'month': 'month',
            'm': 'month',
        }
        models = [
            {
                'name': 'group',
                'key': 'group',
                'type': 'enum',
                'enum_values': list(self.groupings.keys()),
                'default': 'fy',
            }
        ]
        advanced_search_filters = [
            model for model in copy.deepcopy(AWARD_FILTER) if model['name'] in ('time_period', 'recipient_id')
        ]

        for model in advanced_search_filters:
            if model['name'] in ('time_period', 'recipient_id'):
                model['optional'] = False
        models.extend(advanced_search_filters)
        return TinyShield(models).block(json_payload)

    def database_data_layer(self):
        recipient_hash = self.filters['recipient_id'][:-2]
        time_ranges = []
        for t in self.filters['time_period']:
            t['date_type'] = 'action_date'
            time_ranges.append(t)
        queryset = SummaryAwardRecipient.objects.filter()
        queryset &= combine_date_range_queryset(
            time_ranges, SummaryAwardRecipient, API_SEARCH_MIN_DATE, API_MAX_DATE
        )

        if self.filters['recipient_id'][-1] == 'P':
            # there *should* only one record with that hash and recipient_level = 'P'
            parent_duns_rows = RecipientProfile.objects.filter(
                recipient_hash=recipient_hash, recipient_level='P'
            ).values('recipient_unique_id')
            if len(parent_duns_rows) != 1:
                raise InvalidParameterException('Provided recipient_id has no parent records')
            parent_duns = parent_duns_rows[0]['recipient_unique_id']
            queryset = queryset.filter(parent_recipient_unique_id=parent_duns)
        elif self.filters['recipient_id'][-1] == 'R':
            queryset = queryset.filter(recipient_hash=recipient_hash, parent_recipient_unique_id__isnull=True)
        else:
            queryset = queryset.filter(recipient_hash=recipient_hash, parent_recipient_unique_id__isnull=False)

        values = ['fy']
        if self.groupings[self.json_request['group']] == 'month':
            queryset = queryset.annotate(month=FiscalMonth('action_date'), fy=FiscalYear('action_date'))
            values.append('month')

        elif self.groupings[self.json_request['group']] == 'quarter':
            queryset = queryset.annotate(quarter=FiscalQuarter('action_date'), fy=FiscalYear('action_date'))
            values.append('quarter')

        elif self.groupings[self.json_request['group']] == 'fiscal_year':
            queryset = queryset.annotate(fy=FiscalYear('action_date'))

        queryset = queryset.values(*values).annotate(count=Count('award_id'))
        return queryset, values

    @cache_response()
    def post(self, request):
        self.json_request = self.validate_api_request(request.data)
        self.filters = self.json_request.get('filters', None)

        if self.filters is None:
            raise InvalidParameterException('Missing request parameters: filters')

        queryset, values = self.database_data_layer()

        results = bolster_missing_time_periods(
            filter_time_periods=self.filters['time_period'],
            queryset=queryset,
            date_range_type=values[-1],
            columns={'new_award_count_in_period': 'count'})
        return Response({'group': self.groupings[self.json_request['group']], 'results': results})
