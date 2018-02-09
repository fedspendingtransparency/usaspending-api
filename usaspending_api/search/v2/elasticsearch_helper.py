from time import perf_counter
import logging
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError, ConnectionError
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import award_categories, indices_to_award_types
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import award_type_mapping
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP

logger = logging.getLogger('console')
ES_HOSTNAME = settings.ES_HOSTNAME
TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT
DOWNLOAD_QUERY_SIZE = settings.DOWNLOAD_QUERY_SIZE
CLIENT = Elasticsearch(ES_HOSTNAME)

TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})


def swap_keys(dictionary_):
    return dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                for (old_key, new_key) in dictionary_.items())


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
        return None

    try:
        start = perf_counter()
        print('======================================')
        print(query)
        response = CLIENT.search(index=index_name, body=query)
        print('client search took {}s'.format(perf_counter() - start))
    except Exception:
        logger.exception("There was an error connecting to the ElasticSearch instance.")
        return None
    total = response['hits']['total']
    results = format_for_frontend(response['hits']['hits'])
    return results, total, transaction_type


def get_total_results(keyword, index_name):
    index_name = '{}-{}'.format(TRANSACTIONS_INDEX_ROOT,
                                index_name.replace('_', '')) + '*'
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
        logger.exception("There was an error connecting to the ElasticSearch instance.")
        return None


def spending_by_transaction_count(filters):
    keyword = filters['keyword']
    response = {}
    for category in award_categories:
        response[category] = get_total_results(keyword, category)
    return response


def get_sum_aggregation_results(keyword, field='transaction_amount'):
    """
    Size has to be zero here because you only want the aggregations
    """
    index_name = '{}-'.format(TRANSACTIONS_INDEX_ROOT.replace('_', '')) + '*'
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
    try:
        response = CLIENT.search(index=index_name, body=query)
        return response['aggregations']
    except Exception:
        logger.exception("There was an error connecting to the ElasticSearch instance.")
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
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT.replace('_', ''))
    n_iter = DOWNLOAD_QUERY_SIZE // size
    total = None
    # Changed from (potentially an infinite loop to a max of 10 attempts)
    max_iterations = 10
    for i in max_iterations:
        total = get_total_results(keyword, '*')
        if total:
            break
    else:
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

        for i in max_iterations:
            try:
                response = CLIENT.search(index=index_name, body=query, timeout='3m')
                break
            except (TransportError, ConnectionError) as e:
                logger.error(e)
                logger.error('Error retrieving ids. Retrying connection.')
        else:
            logger.error('Error retrieving ids. Max number of retries reached')
            raise
        results = []
        for result in response['aggregations']['results']['buckets']:
            results.append(result['key'])
        yield results
