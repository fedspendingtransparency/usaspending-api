import logging
import copy

from rest_framework.response import Response
from django.db.models import Q, Sum, Count

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.models_matviews import SummaryTransactionRecipientView
from usaspending_api.references.models import RecipientLookup

from usaspending_api.recipient.models import DUNS
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters

logger = logging.getLogger(__name__)

def validate_hash(hash):
    if not RecipientLookup.objects.filter(recipient_hash=hash).count() > 0:
        raise InvalidParameterException('Recipient ID not found: {}.'.format(hash))


def validate_duns(duns):
    if not (isinstance(duns, str) and len(duns)==9):
        raise InvalidParameterException('Invalid DUNS: {}.'.format(duns))
    elif not DUNS.objects.filter(awardee_or_recipient_unique=duns).count() > 0:
        raise InvalidParameterException('DUNS not found: {}.'.format(duns))

def obtain_recipient_totals(recipient_hash, year=None, award_type_codes=None, subawards=False):
    filters = reshape_filters(duns=recipient_hash, year=year, award_type_codes=award_type_codes)

    if not subawards:
        queryset = matview_search_filter(filters, SummaryTransactionRecipientView)
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
        order_by = '-total' if sort == 'desc' else 'total'
        queryset = matview_search_filter(filters, SummaryTransactionRecipientView)\
            .values('recipient_hash', 'recipient_unique_id', 'recipient_name')\
            .annotate(total=Sum('generated_pragmatic_obligation'), count=Count('recipient_hash')).order_by(order_by)
        total_count = queryset.count()
        if limit:
            queryset = queryset[(page-1)*limit: page*limit]


        results = []
        for row in list(queryset):
            recipient_level = 'blah'
            results.append(
                {
                    'hash': '{}-{}'.format(row['recipient_hash'], recipient_level),
                    'duns': row['recipient_unique_id'],
                    'name': row['recipient_name'],
                    'recipient_level': recipient_level,
                    # 'state_province': row['legal_entity_state_code'],
                    'total': row['total'],
                    'count': row['count'],
                }
            )

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


# class ListRecipients(APIDocumentationView):
#
#     @cache_response()
#     def post(self, request):
#         models = [
#             {'name': 'keyword', 'key': 'keyword', 'type': 'text', 'text_type': 'search'},
#             {'name': 'award_type', 'key': 'award_type', 'type': 'text', 'text_type': 'search', 'default': 'all'},
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
#         recipients, page_metadata = get_all_recipients(year='latest', sort=validated_payload['order'],
#                                                        award_type_codes=award_type_codes,
#                                                        page=validated_payload['page'],
#                                                        limit=validated_payload['limit'])
#         for item in recipients:
#             results.append({
#                 'id': item['hash'],
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'recipient_level': item['recipient_level'],
#                 'amount': item['total'],
#                 'count': item['count']
#             })
#
#         return Response({'page_metadata': page_metadata, 'results': results})


class RecipientOverView(APIDocumentationView):

    @cache_response()
    def get(self, request, recipient_hash):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        recipient_hash = validate_hash(recipient_hash)

        # Gather totals
        recipient_totals = obtain_recipient_totals(recipient_hash, year=year)
        # recipient_sub_totals = obtain_recipient_totals(duns, year=year, subawards=True)

        result = {
            # 'name': duns_obj.legal_business_name,
            # 'duns': duns,
            'recipient_level': '',
            # 'parent_name': duns_obj.ultimate_parent_legal_enti,
            # 'parent_duns': duns_obj.ultimate_parent_unique_ide,
            # 'location': location,
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
#                 'id': item['hash'],
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'state_province': item['state_province'],
#                 'amount': item['amount'],
#             })
#         return Response(results)

