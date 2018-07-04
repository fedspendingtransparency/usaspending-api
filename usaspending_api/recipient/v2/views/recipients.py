import logging
import copy

from rest_framework.response import Response
from django.db.models import Q

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings

from usaspending_api.recipient.models import DUNS
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters

logger = logging.getLogger(__name__)


def validate_duns(duns):
    # Since DUNS are more commonly added than states/fips, we can't rely on a cache
    for type, field in {'duns': 'awardee_or_recipient_uniqu', 'name': 'legal_business_name'}.items():
        if DUNS.objects.filter(**{field: duns}).count() > 0:
            return duns, type
    raise InvalidParameterException('DUNS not found: {}.'.format(duns))

def obtain_recipient_totals(duns, year=None, award_type_codes=None, subawards=False):
    filters = reshape_filters(duns=duns, year=year, award_type_codes=award_type_codes)

    if not subawards:
        # TODO: calculate totals filtered by recipient
        queryset = None

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


def get_all_recipients(year=None, award_type_codes=None, subawards=False, parent_duns=None, sort='desc', page=1,
                       limit=None):
    filters = reshape_filters(year=year, award_type_codes=award_type_codes)

    if not subawards:
        # TODO: calculate totals filtered by recipient
        queryset = None
        if limit:
            queryset = queryset[(page-1)*limit: page*limit]

        total_count = queryset.count()

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

        page_metadata = get_simple_pagination_metadata(total_count, limit, page)
    return results, page_metadata


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
        duns_obj = DUNS.objects.filter(awardee_or_recipient_uniqu=duns)
        location = extract_location(duns)

        # Gather totals
        recipient_totals = obtain_recipient_totals(duns, year=year)
        # recipient_sub_totals = obtain_recipient_totals(duns, year=year, subawards=True)

        result = {
            'name': duns_obj.legal_business_name,
            'duns': duns,
            'recipient_level': '',
            'parent_name': duns_obj.ultimate_parent_legal_enti,
            'parent_duns': duns_obj.ultimate_parent_unique_ide,
            'location': location,
            'business_types': '',
            'total_prime_amount': recipient_totals['total'],
            'total_prime_awards': recipient_totals['count'],
            # 'total_sub_amount': recipient_sub_totals['total'],
            # 'total_sub_awards': recipient_sub_totals['count']
        }

        return Response(result)


# class ChildRecipients(APIDocumentationView):
#
#     @cache_response()
#     def get(self, request, duns):
#         get_request = request.query_params
#         year = validate_year(get_request.get('year', 'latest'))
#         duns = validate_duns(duns)
#
#         results = []
#         recipients, page_metadata = get_all_recipients(year=year, parent_duns=duns)
#         for item in recipients:
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
#
#
# class ListRecipients(APIDocumentationView):
#
#     @cache_response()
#     def post(self, request):
#         models = [
#             {'name': 'keyword', 'key': 'keyword', 'type': 'text'},
#             {'name': 'award_type', 'key': 'award_type', 'default': 'all'},
#         ]
#         models.extend(copy.deepcopy(PAGINATION))
#         validated_payload = TinyShield(models).block(request.data)
#
#         # convert award_type -> award_type_codes
#         award_type_codes = None
#         if validated_payload.get('award_type', 'all') != 'all':
#             award_type_codes = all_award_types_mappings[validated_payload['award_type']]
#
#         results = []
#         recipients, page_metadata = get_all_recipients(year='latest', sort=validated_payload['sort'],
#                                                        award_type_codes=award_type_codes,
#                                                        page=validated_payload['page'],
#                                                        limit=validated_payload['limit'])
#         for item in recipients:
#             results.append({
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'recipient_level': item['recipient_level'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#
#         return Response({'page_metadata': page_metadata, 'results': results})
