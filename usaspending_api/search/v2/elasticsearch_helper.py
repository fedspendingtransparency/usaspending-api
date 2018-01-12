from django.conf import settings
from elasticsearch import Elasticsearch
from usaspending_api.awards.v2.lookups.elasticsearch_lookups \
        import TRANSACTIONS_LOOKUP
import logging
logger = logging.getLogger('console')
ES_HOSTNAME = settings.ES_HOSTNAME
TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT


CLIENT = Elasticsearch(ES_HOSTNAME)
TRANSACTIONS_LOOKUP.update({v:k for k, v in
                            TRANSACTIONS_LOOKUP.items()}
                          )
from pprint import pprint


def format_for_frontend(response):
    '''calls reverse key from TRANSACTIONS_LOOKUP '''
    response = [result['_source'] for result in response]
    swap_keys = lambda dictionary_: dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                                         for (old_key, new_key) in dictionary_.items())
    return [swap_keys(result) for result in response]

def search_transactions(filters, fields, sort, order, lower_limit, limit):
    ''' 
    filters: dictionary
    fields: list
    sort: string
    lower_limit: integer
    limit: integer
    '''
    keyword = filters['keyword']
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in fields]
    query_sort = TRANSACTIONS_LOOKUP[sort]

    query = {
        '_source' : query_fields,
        'from' : lower_limit,
        'size' : limit,
        'query': {
            'query_string': {
                'query': keyword
                }
            },
        'sort' : [{
            query_sort : {
                'order' :order}
            }]
        }
    index_name = '{}-{}'.format(TRANSACTIONS_INDEX_ROOT,
                                filters['transaction_type'].lower().replace(' ', ''))
    if index_name[-1] == 's':
        index_name = index_name[:-1]+'*'
    try:
        response = CLIENT.search(index=index_name, body=query)
    except:
        return False, 0
    total = response['hits']['total']
    results = format_for_frontend(response['hits']['hits'])
    return results, total
 