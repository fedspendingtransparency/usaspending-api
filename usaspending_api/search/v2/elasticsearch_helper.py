import logging
import re
from django.conf import settings

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import KEYWORD_DATATYPE_FIELDS
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import indices_to_award_types
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import award_categories
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
    """

    keyword = request_data['keyword']
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in request_data['fields']]
    query_fields.extend(['award_id'])
    query_sort = TRANSACTIONS_LOOKUP[request_data['sort']]
    query = {
        '_source': query_fields,
        'from': lower_limit,
        'size': limit,
        'query': base_query(keyword),
        'sort': [{
            query_sort: {
                'order': request_data['order']}
        }]
    }

    for index, award_types in indices_to_award_types.items():
        if sorted(award_types) == sorted(request_data['award_type_codes']):
            index_name = '{}-{}*'.format(TRANSACTIONS_INDEX_ROOT, index)
            break
    else:
        logger.exception('Bad/Missing Award Types. Did not meet 100% of a category\'s types')
        return False, 'Bad/Missing Award Types requested', None

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


def clean_sub_index(sub_index):
    lookup = {"loans": "loans", "other": "other", "contract":
              "contracts", "direct": "direct_payments", "grant": "grants", "insurance": "insurance"}
    try:
        return lookup[(sub_index)]
    except KeyError:
        logger.error('Unexpected Response')
        return None


def category_aggregation_query(keyword):
    query = {
            "query": {"query_string": {"query": keyword}},
            "aggs": {
                "award_category": {
                    "terms": {
                        "field": "award_category"
                    }
                },
                "pulled_from": {
                    "terms": {
                        "field": "pulled_from"
                    }
                }
            },
            "size": 0
        }
    return query


def spending_by_transaction_count(filters, retries=3):
    index_name = '{}*'.format(TRANSACTIONS_INDEX_ROOT)
    keyword = filters['keyword']
    query = category_aggregation_query(keyword)
    response = es_client_query(index=index_name, body=query, retries=retries)
    results = {key: 0 for key in award_categories}
    pulled_from_idv = 0
    insurance_count = 0
    if response:
        try:
            pulled_from_buckets = response['aggregations']["pulled_from"]["buckets"]
            for bucket in pulled_from_buckets:
                if bucket["key"] == "idv":
                    pulled_from_idv += bucket["doc_count"]
            award_category_buckets = response['aggregations']["award_category"]["buckets"]
            for bucket in award_category_buckets:
                if bucket["key"] == "insurance":
                    insurance_count += bucket["doc_count"]
                else:
                    results[clean_sub_index(bucket["key"])] = bucket["doc_count"]
            results["contracts"] += pulled_from_idv
            results["other"] += insurance_count
            return results
        except KeyError:
            logger.error('Was unable to parse aggregation responses.')
    else:
        return None


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
