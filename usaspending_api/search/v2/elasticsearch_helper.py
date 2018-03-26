import logging
import re
from django.conf import settings

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import KEYWORD_DATATYPE_FIELDS
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import indices_to_award_types
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP
from usaspending_api.core.elasticsearch.client import es_client_query
logger = logging.getLogger('console')

TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT
DOWNLOAD_QUERY_SIZE = settings.DOWNLOAD_QUERY_SIZE
KEYWORD_DATATYPE_FIELDS = ['{}.raw'.format(i) for i in KEYWORD_DATATYPE_FIELDS]

TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})


def preprocess(keyword):
    """Remove Lucene special characters instead of escaping for now"""
    processed_string = re.sub('[\/:\]\[\^!]', '', keyword)
    if len(processed_string) != len(keyword):
        msg = 'Stripped characters from ES keyword search string New: \'{}\' Original: \'{}\''
        logger.info(msg.format(processed_string, keyword))
        keyword = processed_string
    return keyword


def swap_keys(dictionary_):
    return dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                for (old_key, new_key) in dictionary_.items())


def format_for_frontend(response):
    """ calls reverse key from TRANSACTIONS_LOOKUP """
    response = [result['_source'] for result in response]
    return [swap_keys(result) for result in response]


def search_transactions_contracts_query(query_fields, lower_limit, limit, keyword, types, query_sort, request_data):
    query = {
            "_source": query_fields,
            "from": lower_limit,
            "size": limit,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": preprocess(keyword)}},
                        {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "terms": {
                                                        "type": [i.lower() for i in types]
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "term": {
                                                        "pulled_from": "idv"
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
    return query


def base_query(keyword, fields=KEYWORD_DATATYPE_FIELDS):
    keyword = preprocess(keyword)
    query = {
            "dis_max": {
                "queries": [{
                  'query_string': {
                        'query': keyword
                        }
                    },
                    {
                      "query_string": {
                            "query": keyword,
                            "fields": fields
                        }
                    }
                ]
            }
        }
    return query


def search_transactions(request_data, lower_limit, limit):
    """
    filters: dictionary
    fields: list
    sort: string
    order: string
    lower_limit: integer
    limit: integer

    if transaction_type_code not found, return results for contracts

    the two queries below are for contracts and all other award types.
    """
    keyword = request_data['keyword']
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in request_data['fields']]
    query_fields.extend(['award_id'])
    query_sort = TRANSACTIONS_LOOKUP[request_data['sort']]
    types = request_data['award_type_codes']
    for index, award_types in indices_to_award_types.items():
        if sorted(award_types) == sorted(request_data['award_type_codes']):
            index_name = '{}*'.format(TRANSACTIONS_INDEX_ROOT)
            break
    else:
        logger.exception('Bad/Missing Award Types. Did not meet 100% of a category\'s types')
        return False, 'Bad/Missing Award Types requested', None
    if any(x in types for x in ["A", "B", "C", "D"]):
        query = search_transactions_contracts_query(query_fields,
                                                    lower_limit, limit,
                                                    keyword, types,
                                                    query_sort, request_data)
    else:
        query = {
                    "_source": query_fields,
                    "from": lower_limit,
                    "size": limit,
                    "query": {
                        "bool": {
                            "must": [
                                      {"query_string": {"query": preprocess(keyword)}}],
                            "filter": {
                                "terms": {
                                    "type": types
                                    }
                                  }
                                }
                              },
                            "sort": [{
                                  query_sort: {
                                      "order": request_data['order']}
                              }]
                            }
    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        total = response['hits']['total']
        results = format_for_frontend(response['hits']['hits'])
        return True, results, total
    else:
        return False, 'There was an error connecting to the ElasticSearch cluster', None


def get_total_results(keyword, sub_index, retries=3):
    index_name = '{}-{}*'.format(TRANSACTIONS_INDEX_ROOT, sub_index.replace('_', ''))
    query = {'query': base_query(keyword)}

    response = es_client_query(index=index_name, body=query, retries=retries)
    if response:
        try:
            return response['hits']['total']
        except KeyError:
            logger.error('Unexpected Response')
    else:
        logger.error('No Response')
        return None


def spending_by_transaction_count(request_data):
    keyword = request_data['keyword']
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
        'query': base_query(keyword),
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
    if total is None:
        logger.error('Error retrieving total results. Max number of attempts reached')
        return
    required_iter = (total // size) + 1
    n_iter = min(max(1, required_iter), n_iter)
    for i in range(n_iter):
        query = {
            "_source": [field],
            "query": base_query(keyword),
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
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    query = {
        "query": base_query(keyword),
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
        },
        "size": 0}
    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        try:
            results = {}
            results["prime_awards_count"] = response['aggregations']["prime_awards_count"]["value"]
            results["prime_awards_obligation_amount"] = \
                round(response['aggregations']["prime_awards_obligation_amount"]["value"], 2)
            return results
        except KeyError:
            logger.error('Unexpected Response')
    else:
        return None


def spending_by_transaction_sum_and_count(request_data):
    return get_sum_and_count_aggregation_results(request_data['keyword'])
