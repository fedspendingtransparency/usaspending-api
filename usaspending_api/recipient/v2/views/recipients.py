import logging

from rest_framework.response import Response
from django.db.models import Q, F, Sum, Count

from usaspending_api.awards.models_matviews import SummaryTransactionView, UniversalTransactionView
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.recipient.models import RecipientProfile, RecipientLookup

from usaspending_api.recipient.models import DUNS
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters

logger = logging.getLogger(__name__)


def obtain_recipient_totals():
    raise NotImplementedError('yee')


def validate_hash(hash):
    if not RecipientLookup.objects.filter(recipient_hash=hash).count() > 0:
        raise InvalidParameterException('Recipient ID not found: {}.'.format(hash))


def validate_duns(duns):
    if not (isinstance(duns, str) and len(duns) == 9):
        raise InvalidParameterException('Invalid DUNS: {}.'.format(duns))
    elif not DUNS.objects.filter(awardee_or_recipient_unique=duns).count() > 0:
        raise InvalidParameterException('DUNS not found: {}.'.format(duns))


def get_recipients(year=None, hash=None, duns=None, parent_duns=None, subawards=None):
    duns_list = []
    if duns:
        duns_list.append()
    if parent_duns:
        # TODO: Generate list of children via recipient profile table
        duns_list = []

    if year == 'latest' or year is None:
        # Use the Recipient Profile View
        filters = Q()  # recipient_profile_filters()
        total_field = 'last_12_months' if year == 'latest' else 'all_fiscal_years'
        queryset = RecipientProfile.objects.filter(filters).annotate(total=F(total_field)) \
            .values('recipient_level', 'recipient_hash', 'recipient_unique_id', 'recipient_name', 'total')
    else:
        # Use the Universal Transaction Matview for specific years
        filters = reshape_filters(year=year, subawards=subawards)
        queryset = matview_search_filter(filters, UniversalTransactionView) \
            .values('recipient_level', 'recipient_hash') \
            .annotate(total=Sum('generated_pragmatic_obligation'), count=Count()) \
            .values('recipient_level', 'recipient_hash', 'recipient_unique_id', 'recipient_name', 'total')

    results = []
    for row in list(queryset):
        results.append(
            {
                'id': '{}-{}'.format(row['recipient_hash'], row['recipient_level']),
                'duns': row['recipient_unique_id'],
                'name': row['recipient_name'],
                'recipient_level': row['recipient_level'],
                'total': row['total']
            }
        )

    return results


def extract_location(recipient_hash):
    duns = RecipientLookup.objects.filter(recipient_hash=recipient_hash).values('duns').one()
    duns_obj = DUNS.objects.filter(awardee_or_recipient_uniqu=duns)

    return {
        'address_line1': duns_obj.address_line_1,
        'address_line2': duns_obj.address_line_2,
        'address_line3': None,
        'foreign_province': None,
        'city_name': duns_obj.city,
        'county_name': None,
        'state_code': duns_obj.state,
        'zip': duns_obj.zip,
        'zip4': duns_obj.zip4,
        'foreign_postal_code': None,
        'country_name': None,
        'country_code': duns_obj.country_code,
        'congressional_code': duns_obj.congressional_district
    }


def extract_business_types(recipient_hash):
    return SummaryTransactionView.objects.filter(recipient_hash=recipient_hash).values('business_categories')


class RecipientOverView(APIDocumentationView):

    @cache_response()
    def get(self, request, recipient_hash):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        recipient_hash = validate_hash(recipient_hash)

        # Gather DUNS object via the hash
        location = extract_location(recipient_hash)
        business_types = extract_business_types()  # TODO: CONVERT CODES TO READABLE NAMES

        # Gather totals
        recipients, page_metadata = get_recipients(recipient_hash, year=year)
        # sub_recipients, page_metadata = get_recipients(recipient_hash, year=year, subawards=True)

        item = recipients[0]
        duns_obj = DUNS.objects.filter(awardee_or_recipient_uniqu=item['recipient_unique_id'])
        result = {
            'name': item['name'],
            'duns': item['recipient_unique_id'],
            'id': item['id'],
            'recipient_level': item['recipient_level'],
            'parent_name': duns_obj.ultimate_parent_legal_enti,
            'parent_duns': duns_obj.ultimate_parent_unique_ide,
            'business_types': business_types,
            'location': location,
            'total_prime_amount': item['total'],
            # 'total_prime_awards': recipient_totals['count'],
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
#         recipients, page_metadata = get_recipients(year=year, parent_duns=duns)
#         for item in recipients:
#             results.append({
#                 'id': item['hash'],
#                 'name': item['name'],
#                 'duns': item['duns'],
#                 'amount': item['amount'],
#             })
#         return Response(results)
