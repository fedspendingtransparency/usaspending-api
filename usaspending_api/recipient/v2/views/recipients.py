import logging

from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.recipient.models import DUNS
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters

logger = logging.getLogger(__name__)


def obtain_recipient_totals(duns, year=None, award_type_codes=None, subawards=False):
    filters = reshape_filters(duns=duns, year=year, award_type_codes=award_type_codes)

    if not subawards:
        # TODO: calculate totals filtered by recipient
        queryset = []

    try:
        row = list(queryset)[0]
        result = {
            'duns': row['awardee_or_recipient_uniqu'],
            'total': row['total'],
            'count': len(set(row['distinct_awards'].split(','))),
        }
        return result
    except IndexError:
        # would prefer to catch an index error gracefully if the SQL query produces 0 rows
        logger.warn('No results found for DUNS {} with filters: {}'.format(duns, filters))
    return {'count': 0, 'duns': None, 'total': 0}


def get_all_recipients(year=None, award_type_codes=None, subawards=False, sort='duns', page=1):
    filters = reshape_filters(year=year, award_type_codes=award_type_codes)

    if not subawards:
        # TODO: calculate totals filtered by recipient
        queryset = []

        results = [
            {
                'duns': row['awardee_or_recipient_uniqu'],
                'name': row['legal_business_name'],
                'recipient_level': row['recipient_level'],
                'state_province': row['legal_entity_state_code'],
                'total': row['total'],
                'count': len(set(row['distinct_awards'].split(','))),
            }
            for row in list(queryset)]
    return results


def validate_duns(duns):
    # Since DUNS are more commonly added than states/fips, we can't rely on a cache
    available_duns_qs = DUNS.objects.values_list('awardee_or_recipient_uniqu').distinct()
    available_duns = [result[0] for result in available_duns_qs]
    if not (isinstance(duns, str) and len(duns) == 9):
        raise InvalidParameterException('Invalid DUNS: {}.'.format(duns))
    elif duns not in available_duns:
        raise InvalidParameterException('DUNS not found: {}.'.format(duns))


def extract_location(duns):
    return {
        'address_line1': '',
        'address_line2': '',
        'address_line3': '',
        'foreign_province': '',
        'city_name': '',
        'county_name': '',
        'state_code': '',
        'zip': '',
        'zip4': '',
        'foreign_postal_code': '',
        'country_name': '',
        'country_code': '',
        'congressional_code': ''
    }


class RecipientOverView(APIDocumentationView):

    @cache_response()
    def get(self, request, duns):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        duns = validate_duns(duns)

        # Gather general DUNS data

        location = extract_location(duns)

        # Gather totals
        recipient_totals = obtain_recipient_totals(duns, year=year)
        recipient_sub_totals = obtain_recipient_totals(duns, year=year, subawards=True)

        result = {
            'name': '',
            'duns': duns,
            'recipient_level': '',
            'parent_name': '',
            'parent_duns': '',
            'location': location,
            'business_types': '',
            'total_prime_amount': recipient_totals['total'],
            'total_prime_awards': recipient_totals['count'],
            'total_sub_amount': recipient_sub_totals['total'],
            'total_sub_awards': recipient_sub_totals['count']
        }

        return Response(result)


# class ListRecipients(APIDocumentationView):
#
#     @cache_response()
#     def post(self, request):
#         json_request = request.data
#
#         # retrieve search_text from request
#         page = json_request.get('page', 1)
#         sort = json_request.get('sort', 'duns')
#         award_type_codes = json_request.get('award_type_codes', None)
#
#         results = []
#         for item in get_all_recipients(year='latest', sort=sort, award_type_codes=award_type_codes, page=page):
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'recipient_level': item['recipient_level'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
#
# class ChildRecipients(APIDocumentationView):
#
#     @cache_response()
#     def get(self, request):
#         results = []
#         for item in get_all_recipients(year='latest'):
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
#
#
# class RecipientSearch(APIDocumentationView):
#
#     @cache_response()
#     def post(self, request):
#         json_request = request.data
#
#         # retrieve search_text from request
#         search_text = json_request.get('search_text', None)
#
#         results = []
#         for item in get_all_recipients(year='latest', duns=search_text):
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
#
#
# class RecipientAwardsOverTime(APIDocumentationView):
#
#     @cache_response()
#     def post(self, request):
#         json_request = request.data
#
#         # retrieve search_text from request
#         search_text = json_request.get('search_text', None)
#
#         results = []
#         for item in get_all_recipients(year='latest', duns=search_text):
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
