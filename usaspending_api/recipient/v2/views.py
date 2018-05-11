import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import OrderedDict

from rest_framework.response import Response
from usaspending_api.common.views import APIDocumentationView

from django.db.models import Sum

from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
# from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.transaction import transaction_filter
from usaspending_api.awards.v2.filters.matview_filters import universal_transaction_matview_filter
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import table_exists, generate_fiscal_year
from usaspending_api.recipient.models import StateData

logger = logging.getLogger(__name__)


class StateMetaDataViewSet(APIDocumentationView):

    def get_state_data(self, state_data_results, field, year=None):
        """Finds which earliest or latest state data to use based on the year and what data is available"""
        state_data = OrderedDict(sorted([(str(state_data['year']), state_data) for state_data in state_data_results
                                         if state_data[field]], key=lambda pair: pair[0]))
        earliest = list(state_data.keys())[0]
        latest = list(state_data.keys())[-1]
        if year and year.isdigit() and year < earliest:
            return state_data[earliest]
        elif year and year.isdigit() and earliest <= year <= latest:
            return state_data[year]
        elif not year or year > latest or year == 'all' or year == 'latest':
            return state_data[latest]
        else:
            raise InvalidParameterException('Invalid year: {}.'.format(year))

    def get(self, request, fips):
        get_request = request.query_params
        year = get_request.get('year')

        fips = fips.zfill(2)
        state_data_qs = StateData.objects.filter(fips=fips)
        if not state_data_qs.count():
            raise InvalidParameterException('Invalid FIPS ({}) or data unavailable.'.format(fips))

        state_data_results = state_data_qs.values()
        general_state_data = state_data_results[0]

        # recreate filters
        filters = {'place_of_performance_locations': [{'country': 'USA', 'state': general_state_data['code']}]}
        today = datetime.now()
        if year and year.isdigit():
            time_period = [{
                'start_date': '{}-10-01'.format(int(year)-1),
                'end_date': '{}-09-30'.format(year)
            }]
        elif year == 'all':
            time_period = [{
                'start_date': '2008-10-01',
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        elif year == 'latest' or not year:
            last_year = today - relativedelta(years=1)
            time_period = [{
                'start_date': datetime.strftime(last_year, '%Y-%m-%d'),
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        else:
            raise InvalidParameterException('Invalid year: {}.'.format(year))

        filters['time_period'] = time_period
        state_pop_data = self.get_state_data(state_data_results, 'population', year)
        state_mhi_data = self.get_state_data(state_data_results, 'median_household_income', year)

        # calculate award total filtered by state
        if table_exists(UniversalTransactionView):
            total_award_qs = universal_transaction_matview_filter(filters)
        else:
            total_award_qs = transaction_filter(filters)
        total_award_qs = sum_transaction_amount(total_award_qs.values('award_id'))
        total_award_count = total_award_qs.values('award_id').distinct().count()
        total_award_amount = total_award_qs.aggregate(total=Sum('transaction_amount'))['total'] \
            if total_award_count else 0
        if year == 'all' or (year and year.isdigit() and int(year) == generate_fiscal_year(today)):
            amt_per_capita = None
        else:
            amt_per_capita = round(total_award_amount/state_pop_data['population'], 2) if total_award_count else 0

        # calculate subaward total filtered by state - COMMENTED OUT FOR NOW
        # total_subaward_qs = subaward_filter(filters)
        # total_subaward_count = total_subaward_qs.count()
        # total_subaward_amount = total_subaward_qs.aggregate(total=Sum('amount'))['total'] \
        #     if total_subaward_count else 0

        result = {
            'name': general_state_data['name'],
            'code': general_state_data['code'],
            'fips': general_state_data['fips'],
            'type': general_state_data['type'],
            'population': state_pop_data['population'],
            'pop_year': state_pop_data['year'],
            'pop_source': state_pop_data['pop_source'],
            'median_household_income': state_mhi_data['median_household_income'],
            'mhi_year': state_mhi_data['year'],
            'mhi_source': state_mhi_data['mhi_source'],
            'total_prime_amount': total_award_amount,
            'total_prime_awards': total_award_count,
            'award_amount_per_capita': amt_per_capita,
            # Commented out for now
            # 'total_subaward_amount': total_subaward_amount,
            # 'total_subawards': total_subaward_count,
        }

        return Response(result)
