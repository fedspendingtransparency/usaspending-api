'''
Handling elasticsearch queries
'''
import logging
from django.conf import settings
from elasticsearch import Elasticsearch
from usaspending_api.awards.v2.lookups.elasticsearch_lookups \
        import TRANSACTIONS_LOOKUP, award_type_mapping, award_categories
from elasticsearch.exceptions import TransportError, ConnectionError

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
    except Exception:
        logging.exception("There was an error connecting to the ElasticSearch instance.")
        return None
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
    except Exception:
        logging.exception("There was an error connecting to the ElasticSearch instance.")
        return None


def spending_by_transaction_count(filters):
    keyword = filters['keyword']
    response = {}
    for category in award_categories:
        response[category] = get_total_results(keyword, category)
    return response


def search_keyword_id_list_all(keyword):
    """
    althought query size has been increased,
    scrolling may still be time consuming.

    Timeout has been implemented in the
    client to prevent the connection
    from timing out.
    """
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))+'*'
    query = {"query": {"query_string": {"query": keyword}},
             "aggs": {
                    "results": {
                     "terms": {"field": "transaction_id", "size": DOWNLOAD_QUERY_SIZE}
                    }
                }, "size": 0}
    try:
        responses = CLIENT.search(index=index_name, body=query, timeout='3m')
    except Exception:
        logging.exception("There was an error connecting to the ElasticSearch instance.")
        return None
    try:
        responses = responses["aggregations"]['results']
        return [response['key'] for response in responses['buckets']]
    except Exception:
        logging.exception("There was an error parsing the transaction ID's")
        return None


def get_sum_aggregation_results(keyword):
    """
    Size has to be zero here because you only want the aggregations
    """
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))+'*'
    query = {"query": {"query_string": {"query": keyword}},
             "aggs": {"transaction_sum": {"sum": {"field": "transaction_amount"}}}}
    try:
        response = CLIENT.search(index=index_name, body=query)
        return response['aggregations']
    except Exception:
        logging.exception("There was an error connecting to the ElasticSearch instance.")
        return None


def spending_by_transaction_sum(filters):
    keyword = filters['keyword']
    return get_sum_aggregation_results(keyword)


def extract_field_data(response, fieldname):
    '''
    takes in a response body and
    returns list of given
    '''
    hits = response['hits']['hits']
    return [hit['_source'][fieldname] for hit in hits]


def scroll(scroll_id, fieldname):
    '''returns scroll_id and field
    data for a given
     fieldname'''
    response = CLIENT.scroll(scroll_id, scroll='2m')
    results = extract_field_data(response, fieldname)
    scroll_id = response['_scroll_id']
    return results, scroll_id


def get_transaction_ids(keyword, size=100):
    '''returns a generator that
    yields list of transaction ids in chunksize SIZE'''
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))+'*'
    query = {
        "query": {
            "query_string": {
                "query": keyword
            }
        }, "size": size}
    response = CLIENT.search(index=index_name, body=query, scroll='2m', timeout='3m')
    n_iter = DOWNLOAD_QUERY_SIZE//size
    scroll_id = response['_scroll_id']
    for i in range(n_iter):
        results, scroll_id = scroll(scroll_id, 'transaction_id')
        yield results


def get_transaction_ids_agg(keyword, size=10000):
    '''returns a generator that
    yields list of transaction ids in chunksize SIZE'''
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))+'*'
    n_iter = DOWNLOAD_QUERY_SIZE//size
    total = None
    while not total:
        total = get_total_results(keyword, '*')
    n_iter = min(max(1, total//size), n_iter)
    for i in range(n_iter):
        query = {"query": {"query_string": {"query": keyword}},
                 "aggs": {
                        "results": {
                         "terms": {
                             "field": "transaction_id",
                             "include": {
                                 "partition": i,
                                 "num_partitions": n_iter
                             },
                             "size": size}
                        }
                    }, "size": 0}
        found_result = False
        while not found_result:
            try:
                response = CLIENT.search(index=index_name, body=query, scroll='2m', timeout='3m')
                found_result = True
            except (TransportError, ConnectionError) as e:
                logger.error(e)
                logger.error('Error retrieving ids. Retrying connection.')
        results = []
        for result in response['aggregations']['results']['buckets']:
            results.append(result['key'])
        yield results


def get_sum_and_count_aggregation_results(keyword):
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT)+'*'
    query = {"query": {"query_string": {"query": keyword}},
             "aggs": {
              "prime_awards_obligation_amount": {
                 "sum": {
                    "field": "transaction_amount"
                 }
              },
              "prime_awards_count": {
                 "value_count": {
                    "field": "transaction_id"
                 }
              }
              }, "size": 0}
    found_result = 0
    while not found_result == 10:
        try:
            response = CLIENT.search(index=index_name, body=query)
            found_result += 1
            results = {}
            results["prime_awards_count"] = response['aggregations']["prime_awards_count"]["value"]
            results["prime_awards_obligation_amount"] = \
                round(response['aggregations']["prime_awards_obligation_amount"]["value"], 2)
            return results
        except (TransportError, ConnectionError) as e:
            logger.error(e)
            logger.error('Error retrieving ids. Retrying connection.')


def spending_by_transaction_sum_and_count(filters):
    keyword = filters['keyword']
    return get_sum_and_count_aggregation_results(keyword)
