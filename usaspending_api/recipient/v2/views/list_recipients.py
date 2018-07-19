import logging
import copy
from time import perf_counter

from rest_framework.response import Response
from django.db.models import F

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile

logger = logging.getLogger(__name__)


API_TO_DB_MAPPER = {
    'amount': 'total',
    'duns': 'recipient_unique_id',
    'name': 'recipient_name'
}


def get_recipients(year=None, award_type_codes=None, filters={}):
    if year == 'latest' or year is None:
        total_field = 'last_12_months'
        queryset = RecipientProfile.objects \
            .annotate(total=F(total_field)) \
            .values('recipient_level', 'recipient_hash', 'recipient_unique_id', 'recipient_name', 'total')

        if filters['order'] == "desc":
            queryset = queryset.order_by(F(API_TO_DB_MAPPER[filters['sort']]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(API_TO_DB_MAPPER[filters['sort']]).asc(nulls_last=True))
    else:
        raise Exception('Date Range Unsuported!!!!!!!!!!!')

    from usaspending_api.common.helpers.generic_helper import generate_raw_quoted_query
    print('=======================================')
    print(generate_raw_quoted_query(queryset))

    lower_limit = (filters['page'] - 1) * filters['limit']
    upper_limit = filters['page'] * filters['limit']

    count = queryset.count()

    results = []
    page_metadata = get_pagination_metadata(count, filters['limit'], filters['page'])

    for row in queryset[lower_limit:upper_limit + 1]:
        results.append(
            {
                'id': '{}-{}'.format(row['recipient_hash'], row['recipient_level']),
                'duns': row['recipient_unique_id'],
                'name': row['recipient_name'],
                'recipient_level': row['recipient_level'],
                'total': row['total']
            }
        )

    return results, page_metadata


class ListRecipients(APIDocumentationView):

    @cache_response()
    def post(self, request):
        start = perf_counter()
        award_types = list(all_award_types_mappings.keys()) + ['all']
        models = [
            {'name': 'keyword', 'key': 'keyword', 'type': 'text', 'text_type': 'search'},
            {'name': 'award_type', 'key': 'award_type', 'type': 'enum', 'enum_values': award_types, 'default': 'all'},
        ]
        models.extend(copy.deepcopy(PAGINATION))  # page, limit, sort, order

        for model in models:
            if model['name'] == 'sort':
                model['type'] = 'enum'
                model['enum_values'] = ['name', 'duns', 'amount']
                model['default'] = 'amount'
        validated_payload = TinyShield(models).block(request.data)

        # convert award_type -> award_type_codes
        award_type_codes = None
        if validated_payload['award_type'] != 'all':
            award_type_codes = all_award_types_mappings[validated_payload['award_type']]

        results = []
        recipients, page_metadata = get_recipients(filters=validated_payload, award_type_codes=award_type_codes)
        for item in recipients:
            results.append({
                'id': item['id'],
                'duns': item['duns'],
                'name': item['name'],
                'recipient_level': item['recipient_level'],
                'amount': item['total']
            })
        print(f'total time taken: {perf_counter() - start}')
        return Response({'page_metadata': page_metadata, 'results': results})
