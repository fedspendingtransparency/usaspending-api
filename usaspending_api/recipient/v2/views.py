import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import OrderedDict

from rest_framework.response import Response
from usaspending_api.common.views import APIDocumentationView

from django.db.models import Sum

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings as ats
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
# from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.matview_filters import universal_transaction_matview_filter
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import generate_fiscal_year
from usaspending_api.recipient.models import StateData

logger = logging.getLogger(__name__)

# Storing FIPS codes + state codes in memory to avoid hitting the database for the same data
VALID_FIPS = {}


def populate_fips():
    global VALID_FIPS

    if not VALID_FIPS:
        VALID_FIPS = {fips_code: state_code for fips_code, state_code
                      in list(StateData.objects.distinct('fips').values_list('fips', 'code'))}


def validate_fips(fips):
    global VALID_FIPS
    populate_fips()

    if fips not in VALID_FIPS:
        raise InvalidParameterException('Invalid fips: {}.'.format(fips))

    return fips


def validate_year(year=None):
    if year and not (year.isdigit() or year in ['all', 'latest']):
        raise InvalidParameterException('Invalid year: {}.'.format(year))
    return year


def recreate_filters(state_code='', year=None, award_type_codes=None):
    # recreate filters
    filters = {}

    if state_code:
        filters['place_of_performance_locations'] = [{'country': 'USA', 'state': state_code}]

    if year:
        today = datetime.now()
        if year and year.isdigit():
            time_period = [{
                'start_date': '{}-10-01'.format(int(year) - 1),
                'end_date': '{}-09-30'.format(year)
            }]
        elif year == 'all':
            time_period = [{
                'start_date': '2008-10-01',
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        else:
            last_year = today - relativedelta(years=1)
            time_period = [{
                'start_date': datetime.strftime(last_year, '%Y-%m-%d'),
                'end_date': datetime.strftime(today, '%Y-%m-%d')
            }]
        filters['time_period'] = time_period

    if award_type_codes:
        filters['award_type_codes'] = award_type_codes

    return filters


def calcuate_totals(fips, year=None, award_type_codes=None, subawards=False):
    filters = recreate_filters(state_code=VALID_FIPS[fips], year=year, award_type_codes=award_type_codes)

    if not subawards:
        # calculate award total filtered by state
        total_award_qs = universal_transaction_matview_filter(filters)
        # total_award_qs = sum_transaction_amount(total_award_qs.values('award_id'))
        count = total_award_qs.values('award_id').distinct().count()
        amount = total_award_qs.aggregate(total=Sum('transaction_amount'))['total'] if count else 0
    else:
        # calculate subaward total filtered by state - COMMENTED OUT FOR NOW
        # total_subaward_qs = subaward_filter(filters)
        # count = total_subaward_qs.count()
        # amount = total_subaward_qs.aggregate(total=Sum('amount'))['total'] \
        #     if count else 0
        pass
    return count, amount


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
        else:
            return state_data[latest]

    def get(self, request, fips):
        get_request = request.query_params
        year = validate_year(get_request.get('year'))
        fips = validate_fips(fips)

        state_data_qs = StateData.objects.filter(fips=fips)
        state_data_results = state_data_qs.values()
        general_state_data = state_data_results[0]
        state_pop_data = self.get_state_data(state_data_results, 'population', year)
        state_mhi_data = self.get_state_data(state_data_results, 'median_household_income', year)

        total_award_count, total_award_amount = calcuate_totals(fips, year=year)
        if year == 'all' or (year and year.isdigit() and int(year) == generate_fiscal_year(datetime.now())):
            amt_per_capita = None
        else:
            amt_per_capita = round(total_award_amount/state_pop_data['population'], 2) if total_award_count else 0

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

class StateAwardBreakdownViewSet(APIDocumentationView):

    def get(self, request, fips):
        get_request = request.query_params
        year = validate_year(get_request.get('year'))
        fips = validate_fips(fips)

        results = []
        for award_type in ats:
            total_award_count, total_award_amount = calcuate_totals(fips, year=year, award_type_codes=ats[award_type])
            results.append({
                'type': award_type,
                'amount': total_award_amount,
                'count': total_award_count
            })
        return Response(results)

class ListStates(APIDocumentationView):

    def get(self, request):
        populate_fips()

        results = []
        for fips, state_code in VALID_FIPS.items():
            total_award_count, total_award_amount = calcuate_totals(fips, year='latest')
            results.append({
                'fips': fips,
                'state_code': state_code,
                'amount': total_award_amount
            })
        return Response(results)