'''
Handling elasticsearch queries
'''
import logging
from django.conf import settings
from elasticsearch import Elasticsearch
from usaspending_api.awards.v2.lookups.elasticsearch_lookups \
        import TRANSACTIONS_LOOKUP, award_type_mapping, award_categories


logger = logging.getLogger('console')
ES_HOSTNAME = settings.ES_HOSTNAME
TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT
DOWNLOAD_QUERY_SIZE = settings.DOWNLOAD_QUERY_SIZE
CLIENT = Elasticsearch(ES_HOSTNAME)
TRANSACTIONS_LOOKUP.update({v: k for k, v in
                            TRANSACTIONS_LOOKUP.items()}
                           )


def swap_keys(dictionary_):
    dictionary_ = dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                       for (old_key, new_key) in dictionary_.items())
    return dictionary_


def format_for_frontend(response):
    '''calls reverse key from TRANSACTIONS_LOOKUP '''
    response = [result['_source'] for result in response]
    return [swap_keys(result) for result in response]


def search_transactions(filters, fields, sort, order, lower_limit, limit):
    '''
    filters: dictionary
    fields: list
    sort: string
    order: string
    lower_limit: integer
    limit: integer

    if transaction_type_code not found, return results for contracts
    '''
    keyword = filters['keyword']
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in fields]
    query_fields.extend(['piid', 'fain'])
    query_sort = TRANSACTIONS_LOOKUP[sort]
    query = {
        '_source': query_fields,
        'from': lower_limit,
        'size': limit,
        'query': {
            'query_string': {
                'query': keyword
                }
            },
        'sort': [{
            query_sort: {
                'order': order}
            }]
        }

    transaction_type = next((award_type_mapping[k] for k in
                             filters['award_type_codes']
                             if k in award_type_mapping), 'contracts')

    index_name = '{}-{}'.format(TRANSACTIONS_INDEX_ROOT,
                                transaction_type.lower().replace(' ', ''))
    index_name = index_name[:-1]+'*'
    try:
        response = CLIENT.search(index=index_name, body=query)
    except Exception as es1:
        return False, -1
    total = response['hits']['total']
    results = format_for_frontend(response['hits']['hits'])
    return results, total, transaction_type


def get_total_results(keyword, index_name):
    index_name = '{}-{}'.format(TRANSACTIONS_INDEX_ROOT,
                                index_name.replace('_', ''))+'*'
    query = {
        'query': {
            'query_string': {
                'query': keyword
                }
            }
    }
    try:
        response = CLIENT.search(index=index_name, body=query)
        return response['hits']['total']
    except Exception as es1:
        return -1


def spending_by_transaction_count(filters):
    keyword = filters['keyword']
    response = {}
    for category in award_categories:
        response[category] = get_total_results(keyword, category)
    return response

def search_keyword_id_list_all(keyword):
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))+'*'
    print(index_name)
    
    query = {
        "query": {
            "query_string" : {
                "query" : keyword
            }
        },"size": DOWNLOAD_QUERY_SIZE
    }
    
    try:
        response = CLIENT.search(index=index_name, body=query, scroll = '2m')
    except Exception as es1:
        return es1
    
    sid = response['_scroll_id']
    scroll_size = response['hits']['total']
    
    transaction_id_list = []

  # Start scrolling
    while (scroll_size > 0):
        print("Scrolling...")
        response = CLIENT.scroll(scroll_id = sid, scroll = '2m')
        # Update the scroll ID
        sid = response['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(response['hits']['hits'])
        print("scroll size: " + str(scroll_size))
        # Do something with the obtained page   
        new_response = response['hits']['hits']
        for i in (range(DOWNLOAD_QUERY_SIZE)):
            list_item = (new_response[i]['_source']['transaction_id'])
            transaction_id_list.append(list_item)
    
    return transaction_id_list
