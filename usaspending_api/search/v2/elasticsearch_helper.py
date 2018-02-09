import logging
from django.conf import settings
from elasticsearch import Elasticsearch, TransportError

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import indices_to_award_types
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP

logger = logging.getLogger('console')

TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT
DOWNLOAD_QUERY_SIZE = settings.DOWNLOAD_QUERY_SIZE
CLIENT = Elasticsearch(settings.ES_HOSTNAME)
TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})


def swap_keys(dictionary_):
    return dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                for (old_key, new_key) in dictionary_.items())


def format_for_frontend(response):
    '''calls reverse key from TRANSACTIONS_LOOKUP '''
    response = [result['_source'] for result in response]
    return [swap_keys(result) for result in response]


def es_client_query(index, body, timeout='1m', retries=1):
    if retries > 20:
        retries = 20
    elif retries < 1:
        retries = 1
    for attempt in range(retries):
        try:
            response = CLIENT.search(index=index, body=body, timeout=timeout)
        except Exception:
            logger.exception('There was an error connecting to the ElasticSearch cluster.')
            continue
        return response
    logger.error('Error connecting to elasticsearch. {} attempts made'.format(retries))
    return None


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
    query_fields.extend(['piid', 'fain', 'uri', 'display_award_id'])
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

    for index, award_types in indices_to_award_types.items():
        if sorted(award_types) == sorted(filters['award_type_codes']):
            index_name = '{}-{}-*'.format(TRANSACTIONS_INDEX_ROOT, index)
            transaction_type = index
            break
    else:
        logger.exception('Bad/Missing Award Types requested')
        return False, 'Bad/Missing Award Types requested', None, None

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        total = response['hits']['total']
        results = format_for_frontend(response['hits']['hits'])
        return True, results, total, transaction_type
    else:
        return False, 'There was an error connecting to the ElasticSearch cluster', None, None


def get_total_results(keyword, sub_index, retries=3):
    index_name = '{}-{}*'.format(TRANSACTIONS_INDEX_ROOT, sub_index.replace('_', ''))
    query = {
        'query': {
            'query_string': {
                'query': keyword
            }
        }
    }

    response = es_client_query(index=index_name, body=query, retries=retries)
    if response:
        try:
            return response['hits']['total']
        except KeyError:
            logger.error('Unexpected Response')
    else:
        logger.error('No Response')
        return None


def spending_by_transaction_count(filters):
    keyword = filters['keyword']
    response = {}

    for category in indices_to_award_types.keys():
        total = get_total_results(keyword, category)
        if total is not None:
            if category == 'directpayments':
                category = 'direct_payments'
            response[category] = total
        else:
            return total
    return response


def get_sum_aggregation_results(keyword, field='transaction_amount'):
    """
    Size has to be zero here because you only want the aggregations
    """
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    query = {
        'query': {
            'query_string': {
                'query': keyword
            }
        },
        'aggs': {
            'transaction_sum': {
                'sum': {
                    'field': field
                }
            }
        }
    }

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        return response['aggregations']
    else:
        return None


def spending_by_transaction_sum(filters):
    keyword = filters['keyword']
    return get_sum_aggregation_results(keyword)


def get_download_ids(keyword, field, size=10000):
    '''
    returns a generator that
    yields list of transaction ids in chunksize SIZE

    Note: this only works for fields in ES of integer type.
    '''
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    n_iter = DOWNLOAD_QUERY_SIZE // size

    max_iterations = 10
    total = get_total_results(keyword, '*', max_iterations)
    if not total:
        logger.error('Error retrieving total results. Max number of attempts reached')
        return None

    n_iter = min(max(1, total // size), n_iter)
    for i in range(n_iter):
        query = {
            "_source": [field],
            "query": {"query_string": {"query": keyword}},
            "aggs": {
                "results": {
                    "terms": {
                        "field": field,
                        "include": {
                            "partition": i,
                            "num_partitions": n_iter
                        },
                        "size": size
                    }
                }
            },
            "size": 0
        }

        response = es_client_query(index=index_name, body=query, retries=max_iterations, timeout='3m')
        if not response:
            raise Exception('Breaking generator, unable to reach cluster')
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
    while not found_result > 10:
        found_result += 1
        try:
            response = CLIENT.search(index=index_name, body=query)
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
