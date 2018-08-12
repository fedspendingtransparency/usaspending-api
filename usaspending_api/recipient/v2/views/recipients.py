import logging
import uuid

from rest_framework.response import Response
from django.db.models import F, Sum, Count

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.recipient.models import DUNS, RecipientProfile, RecipientLookup
from usaspending_api.recipient.v2.helpers import validate_year, reshape_filters
from usaspending_api.references.models import RefCountryCode, LegalEntity

logger = logging.getLogger(__name__)

# Recipient Levels
#   - P = Parent Recipient, There is at least one child recipient that lists this recipient as a parent
#   - C = Child Recipient, References a parent recipient
#   - R = Recipient, No parent info provided
RECIPIENT_LEVELS = ['P', 'C', 'R']


def validate_recipient_id(recipient_id):
    """ Validate [duns+name]-[recipient_type] hash

        Args:
            hash: str of the hash+duns to look up

        Returns:
            uuid of hash
            recipient level

        Raises:
            InvalidParameterException for invalid hashes
    """
    if '-' not in recipient_id:
        raise InvalidParameterException('ID (\'{}\') doesn\'t include Recipient-Level'.format(hash))
    recipient_level = recipient_id[recipient_id.rfind('-') + 1:]
    if recipient_level not in RECIPIENT_LEVELS:
        raise InvalidParameterException('Invalid Recipient-Level: \'{}\''.format(recipient_level))
    recipient_hash = recipient_id[:recipient_id.rfind('-')]
    try:
        uuid.UUID(recipient_hash)
    except ValueError:
        raise InvalidParameterException('Recipient Hash not valid UUID: \'{}\'.'.format(recipient_hash))
    if not RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level=recipient_level).count():
        raise InvalidParameterException('Recipient ID not found: \'{}\'.'.format(recipient_id))
    return recipient_hash, recipient_level


def extract_name_duns_from_hash(recipient_hash):
    """ Extract the name and duns from the recipient hash

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            duns and name
    """
    name_duns_qs = RecipientLookup.objects.filter(recipient_hash=recipient_hash).values('duns', 'legal_business_name')\
        .first()
    if not name_duns_qs:
        return None, None
    else:
        return name_duns_qs['duns'], name_duns_qs['legal_business_name']


def extract_parent_from_hash(recipient_hash):
    """ Extract the parent name and parent duns from the recipient hash

        Args:
            recipient_hash: uuid of the hash+duns to look up

        Returns:
            parent_duns
            parent_name
    """
    duns = None
    name = None
    parent_id = None
    affiliations = RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='C')\
        .values('recipient_affiliations')
    if not affiliations:
        return duns, name, parent_id
    duns = affiliations[0]['recipient_affiliations'][0]

    parent = RecipientLookup.objects.filter(duns=duns).values('recipient_hash', 'legal_business_name').first()
    if parent:
        name = parent['legal_business_name']
        parent_id = '{}-P'.format(parent['recipient_hash'])
    return duns, name, parent_id


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
        duns, name = extract_name_duns_from_hash(recipient_hash)
        legal_entity = LegalEntity.objects.filter(recipient_name=name,
                                                  recipient_unique_id=duns).\
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


def extract_business_categories(recipient_name, recipient_duns):
    """ Extract the business categories via the recipient hash

        Args:
            recipient_name: name of the recipient
            recipient_duns: duns of the recipient

        Returns:
            list of business categories
    """
    qs_business_cat = LegalEntity.objects.filter(recipient_name=recipient_name, recipient_unique_id=recipient_duns)\
        .order_by('-update_date').values('business_categories').first()
    return qs_business_cat['business_categories'] if qs_business_cat is not None else []


def obtain_recipient_totals(recipient_id, year='latest', subawards=False):
    """ Extract the total amount and transaction count for the recipient_hash given the timeframe

        Args:
            recipient_id: string of hash(duns, name)-[recipient-level]

        Returns:
            total transactions
            total amount
    """
    # Note: We could use the RecipientProfile to get the totals for last 12 months, thought we still need the count.
    filters = reshape_filters(recipient_id=recipient_id, year=year)
    queryset = matview_search_filter(filters, UniversalTransactionView)
    aggregates = queryset.aggregate(total=Sum('generated_pragmatic_obligation'), count=Count('transaction_id'))
    total = aggregates['total'] if aggregates['total'] else 0
    count = aggregates['count'] if aggregates['count'] else 0
    return total, count


class RecipientOverView(APIDocumentationView):

    @cache_response()
    def get(self, request, recipient_id):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        recipient_hash, recipient_level = validate_recipient_id(recipient_id)
        recipient_duns, recipient_name = extract_name_duns_from_hash(recipient_hash)

        if recipient_level != 'R':
            parent_duns, parent_name, parent_id = extract_parent_from_hash(recipient_hash)
        else:
            parent_duns, parent_name, parent_id = None, None, None
        location = extract_location(recipient_hash)
        business_types = extract_business_categories(recipient_name, recipient_duns)
        total, count = obtain_recipient_totals(recipient_id, year=year, subawards=False)
        # subtotal, subcount = obtain_recipient_totals(recipient_hash, recipient_level, year=year, subawards=False)

        result = {
            'name': recipient_name,
            'duns': recipient_duns,
            'recipient_id': recipient_id,
            'recipient_level': recipient_level,
            'parent_id': parent_id,
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


def extract_hash_name_from_duns(duns):
    """ Extract the all the names and hashes associated with the DUNS provided

        Args:
            duns: duns to find the equivalent hash and name

        Returns:
            list of dictionaries containing hashes and names
    """
    qs_hash = RecipientLookup.objects.filter(duns=duns).values('recipient_hash', 'legal_business_name').first()
    if not qs_hash:
        return None, None
    else:
        return qs_hash['recipient_hash'], qs_hash['legal_business_name']


class ChildRecipients(APIDocumentationView):

    @cache_response()
    def get(self, request, duns):
        get_request = request.query_params
        year = validate_year(get_request.get('year', 'latest'))
        parent_hash, parent_name = extract_hash_name_from_duns(duns)
        if not parent_hash:
            raise InvalidParameterException('DUNS not found: \'{}\'.'.format(duns))

        # Get all possible child duns
        children_duns = RecipientProfile.objects.filter(recipient_hash=parent_hash, recipient_level='P').values(
            'recipient_affiliations')
        if not children_duns:
            raise InvalidParameterException('DUNS is not listed as a parent: \'{}\'.'.format(duns))
        children = children_duns[0]['recipient_affiliations']

        # Get child info for each child DUNS
        results = []
        recipient_level = 'C'
        for child_duns in children:
            child_hash, child_name = extract_hash_name_from_duns(child_duns)
            if not child_hash:
                logger.warning('Child Duns Not Found in Recipient Lookup, skipping: {}'.format(child_duns))
                continue
            child_recipient_id = '{}-{}'.format(child_hash, recipient_level)
            total, count = obtain_recipient_totals(child_recipient_id, year=year, subawards=False)
            location = extract_location(child_hash)
            results.append({
                'recipient_id': child_recipient_id,
                'name': child_name,
                'duns': child_duns,
                'amount': total,
                'state_province': location['state_code']
            })
        return Response(results)
