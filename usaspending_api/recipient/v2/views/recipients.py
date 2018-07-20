import logging
import uuid

from rest_framework.response import Response
from django.db.models import Q, F, Sum, Count

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.recipient.models import RecipientProfile, DUNS
from usaspending_api.references.models import RecipientLookup, RefCountryCode, LegalEntity
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters

logger = logging.getLogger(__name__)

RECIPIENT_TYPES = ['C', 'R', 'P']


def validate_hash(hash):
    """ Validate [duns+name]-[recipient_type] hash

        Args:
            hash: str of the hash+duns to look up

        Returns:
            uuid of hash
            recipient type

        Raises:
            InvalidParameterException for invalid hashes
    """
    if '-' not in hash:
        raise InvalidParameterException('ID (\'{}\') doesn\'t include Recipient-Type'.format(hash))
    recipient_type = hash[hash.rfind('-')+1:]
    if recipient_type not in RECIPIENT_TYPES:
        raise InvalidParameterException('Invalid Recipient-Type: \'{}\''.format(recipient_type))
    recipient_hash = hash[:hash.rfind('-')]
    try:
        uuid_hash = uuid.UUID(recipient_hash)
    except ValueError:
        raise InvalidParameterException('Recipient ID not valid UUID: \'{}\'.'.format(recipient_hash))
    if not RecipientLookup.objects.filter(recipient_hash=uuid_hash).count():
        raise InvalidParameterException('Recipient ID not found: \'{}\'.'.format(recipient_hash))
    return uuid_hash, recipient_type


def extract_name_duns_from_hash(recipient_hash):
    """ Extract the name and duns from the recipient hash

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            dict of the corresponding name and duns
    """
    return RecipientLookup.objects.filter(recipient_hash=recipient_hash).values('duns', 'legal_business_name').first()


def extract_parent_from_hash(recipient_hash):
    """ Extract the parent name and parent duns from the recipient hash

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            parent_duns, parent_name
    """
    affiliations = RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='C')\
        .values('recipient_affiliations')
    if not affiliations:
        return None, None
    else:
        duns = affiliations[0]['recipient_affiliations'][0]
        duns_obj = DUNS.objects.filter(awardee_or_recipient_uniqu=duns).values('legal_business_name').first()
        name = duns_obj['legal_business_name']
        return duns, name

def extract_location(recipient_hash):
    """ Extract the location data via the recipient hash

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            dict of location info
    """
    location = {
        'address_line1': None,
        'address_line2': None,
        'address_line3': None,
        'foreign_province': None,
        'city_name': None,
        'county_name': None,
        'state_code': None,
        'zip': None,
        'zip4': None,
        'foreign_postal_code': None,
        'country_name': None,
        'country_code': None,
        'congressional_code': None
    }
    duns = RecipientLookup.objects.filter(recipient_hash=recipient_hash).values('duns').first()
    duns_obj = DUNS.objects.filter(awardee_or_recipient_uniqu=duns['duns']).first() if duns else None
    if duns_obj:
        country_name = (RefCountryCode.objects.filter(country_code=duns_obj.country_code).values('country_name').first()
                        if duns_obj else None)
        location.update({
            'address_line1': duns_obj.address_line_1,
            'address_line2': duns_obj.address_line_2,
            'city_name': duns_obj.city,
            'state_code': duns_obj.state,
            'zip': duns_obj.zip,
            'zip4': duns_obj.zip4,
            'country_name': country_name['country_name'] if country_name else None,
            'country_code': duns_obj.country_code,
            'congressional_code': duns_obj.congressional_district
        })
    else:
        # Extract the location from the latest legal entity
        re_details = extract_name_duns_from_hash(recipient_hash)
        legal_entity = LegalEntity.objects.filter(recipient_name=re_details['legal_business_name'],
                                                  recipient_unique_id=re_details['duns']).\
            order_by('-update_date')\
            .values(
                address_line1=F('location__address_line1'),
                address_line2=F('location__address_line2'),
                address_line3=F('location__address_line3'),
                foreign_province=F('location__foreign_province'),
                city_name=F('location__city_name'),
                county_name=F('location__county_name'),
                state_code=F('location__state_code'),
                zip=F('location__zip4'),
                zip4=F('location__zip_4a'),
                foreign_postal_code=F('location__foreign_postal_code'),
                country_name=F('location__country_name'),
                country_code=F('location__location_country_code'),
                congressional_code=F('location__congressional_code')
        ).first()
        if legal_entity:
            location.update(legal_entity)
    return location


def extract_business_types(recipient_name, recipient_duns):
    """ Extract the location data via the recipient hash

        Args:
            recipient_name: name of the recipient
            recipient_duns: duns of the recipient

        Returns:
            dict of business types info
    """
    business_categories = LegalEntity.objects.filter(recipient_name=recipient_name, recipient_unique_id=recipient_duns)\
        .order_by('-update_date').values('business_categories').first()
    return business_categories


def obtain_recipient_totals(recipient_hash, recipient_level, year='latest', subawards=False):
    """ Extract the total amount and transaction count for the recipient_hash given the timeframe

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            dict of the corresponding name and duns
    """
    # Note: We could use the RecipientProfile to get the totals for last 12 months, thought we still need the count.

    recipient_hashes = []

    if recipient_level == 'P':
        # Add only the children recipients
        qs_affiliations = RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='P')\
            .values('recipient_affiliations')
        affiliations = qs_affiliations[0]['recipient_affiliations']
        qs_children = RecipientLookup.objects.filter(duns__in=affiliations).values('recipient_hash')
        child_hashes = [result['recipient_hash'] for result in qs_children]
        recipient_hashes.extend(child_hashes)
    else:
        # Child or recipient level
        recipient_hashes.append(recipient_hash)

    filters = reshape_filters(recipient_hash=recipient_hashes, year=year)
    queryset = matview_search_filter(filters, UniversalTransactionView) \
        .values('recipient_level', 'recipient_hash') \
        .annotate(total=Sum('generated_pragmatic_obligation'), count=Count('recipient_hash')) \
        .values('recipient_level', 'recipient_hash', 'total', 'count').first()

    return queryset['total'], queryset['count']

class RecipientOverView(APIDocumentationView):

    @cache_response()
    def get(self, request, id):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        recipient_hash, recipient_level = validate_hash(id)
        recipient_name, recipient_duns = extract_name_duns_from_hash(recipient_hash)

        parent_name, parent_duns = extract_parent_from_hash(recipient_hash)
        location = extract_location(recipient_hash)
        business_types = extract_business_types(recipient_name, recipient_duns)
        total, count = obtain_recipient_totals(recipient_hash, recipient_level, year=year, subawards=False)
        return Response({'total': total, 'count': count})
        # subtotal, subcount = obtain_recipient_totals(recipient_hash, recipient_level, year=year, subawards=False)

        result = {
            'name': recipient_name,
            'duns': recipient_duns,
            'id': id,
            'recipient_level': recipient_level,
            'parent_name': parent_name,
            'parent_duns': parent_duns,
            'business_types': business_types,
            'location': location,
            'total_transaction_amount': total,
            'total_transactions': count,
            # 'total_sub_transaction_amount': subtotal,
            # 'total_sub_transaction_total': subcount
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
