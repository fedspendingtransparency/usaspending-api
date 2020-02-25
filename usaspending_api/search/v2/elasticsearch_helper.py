import logging
import re
from typing import Dict

from django.conf import settings
from elasticsearch_dsl import A, Q as ES_Q

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    TRANSACTIONS_LOOKUP,
    TRANSACTIONS_SOURCE_LOOKUP,
    KEYWORD_DATATYPE_FIELDS,
    INDEX_ALIASES_TO_AWARD_TYPES,
)
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch

logger = logging.getLogger("console")

DOWNLOAD_QUERY_SIZE = settings.MAX_DOWNLOAD_LIMIT
TRANSACTIONS_SOURCE_LOOKUP.update({v: k for k, v in TRANSACTIONS_SOURCE_LOOKUP.items()})


def es_sanitize(input_string):
    """ Escapes reserved elasticsearch characters and removes when necessary """

    processed_string = re.sub(r'([-&!|{}()^~*?:\\/"+\[\]<>])', "", input_string)
    if len(processed_string) != len(input_string):
        msg = "Stripped characters from input string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, input_string))
    return processed_string


def es_minimal_sanitize(keyword):
    keyword = concat_if_array(keyword)
    """Remove Lucene special characters instead of escaping for now"""
    processed_string = re.sub(r"[/:][^!]", "", keyword)
    if len(processed_string) != len(keyword):
        msg = "Stripped characters from ES keyword search string New: '{}' Original: '{}'"
        logger.info(msg.format(processed_string, keyword))
        keyword = processed_string
    return keyword


def swap_keys(dictionary_):
    return dict(
        (TRANSACTIONS_SOURCE_LOOKUP.get(old_key, old_key), new_key) for (old_key, new_key) in dictionary_.items()
    )


def format_for_frontend(response):
    """ calls reverse key from TRANSACTIONS_LOOKUP """
    response = [result["_source"] for result in response]
    return [swap_keys(result) for result in response]


def base_query(keyword, fields=KEYWORD_DATATYPE_FIELDS):
    keyword = es_minimal_sanitize(concat_if_array(keyword))
    query = {
        "dis_max": {
            "queries": [{"query_string": {"query": keyword}}, {"query_string": {"query": keyword, "fields": fields}}]
        }
    }
    return query


def search_transactions(request_data, lower_limit, limit):
    """
    request_data: dictionary
    lower_limit: integer
    limit: integer

    if transaction_type_code not found, return results for contracts
    """

    keyword = request_data["filters"]["keywords"]
    query_fields = [TRANSACTIONS_SOURCE_LOOKUP[i] for i in request_data["fields"]]
    query_fields.extend(["award_id", "generated_unique_award_id"])
    query_sort = TRANSACTIONS_LOOKUP[request_data["sort"]]
    query = {
        "_source": query_fields,
        "from": lower_limit,
        "size": limit,
        "query": base_query(keyword),
        "sort": [{query_sort: {"order": request_data["order"]}}],
    }

    for index, award_types in INDEX_ALIASES_TO_AWARD_TYPES.items():
        if sorted(award_types) == sorted(request_data["filters"]["award_type_codes"]):
            index_name = "{}-{}*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX, index)
            break
    else:
        logger.exception("Bad/Missing Award Types. Did not meet 100% of a category's types")
        return False, "Bad/Missing Award Types requested", None

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        total = response["hits"]["total"]["value"]
        results = format_for_frontend(response["hits"]["hits"])
        return True, results, total
    else:
        return False, "There was an error connecting to the ElasticSearch cluster", None


def get_total_results(keyword, retries=3):
    index_name = "{}-*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX)
    aggregations = {
        "types": {
            "filters": {
                "filters": {
                    category: {"terms": {"type": types}} for category, types in all_award_types_mappings.items()
                }
            }
        }
    }
    query = {"query": base_query(keyword), "aggs": aggregations}
    response = es_client_query(index=index_name, body=query, retries=retries)
    if response:
        try:
            return response["aggregations"]["types"]["buckets"]
        except KeyError:
            logger.error("Unexpected Response")
    else:
        logger.error("No Response")
        return None


def spending_by_transaction_count(request_data):
    keyword = request_data["filters"]["keywords"]
    response = {}
    results = get_total_results(keyword)
    for category in INDEX_ALIASES_TO_AWARD_TYPES.keys():
        if results is not None:
            if category == "directpayments":
                category = "direct_payments"
            if category == "other":
                category = "other_financial_assistance"
            response[category] = results[category]["doc_count"]
        else:
            return results
    return response


def get_sum_aggregation_results(keyword, field="transaction_amount"):
    """
    Size has to be zero here because you only want the aggregations
    """
    index_name = "{}-*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX)
    query = {"query": base_query(keyword), "aggs": {"transaction_sum": {"sum": {"field": field}}}}

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        return response["aggregations"]
    else:
        return None


def spending_by_transaction_sum(filters):
    keyword = filters["keywords"]
    return get_sum_aggregation_results(keyword)


def get_download_ids(keyword, field, size=10000):
    """
    returns a generator that
    yields list of transaction ids in chunksize SIZE

    Note: this only works for fields in ES of integer type.
    """
    index_name = "{}-*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX)
    n_iter = DOWNLOAD_QUERY_SIZE // size

    max_iterations = 10
    total = get_total_results(keyword, "*", max_iterations)
    if total is None:
        logger.error("Error retrieving total results. Max number of attempts reached")
        return
    required_iter = (total // size) + 1
    n_iter = min(max(1, required_iter), n_iter)
    for i in range(n_iter):
        query = {
            "_source": [field],
            "query": base_query(keyword),
            "aggs": {
                "results": {
                    "terms": {"field": field, "include": {"partition": i, "num_partitions": n_iter}, "size": size}
                }
            },
            "size": 0,
        }

        response = es_client_query(index=index_name, body=query, retries=max_iterations, timeout="3m")
        if not response:
            raise Exception("Breaking generator, unable to reach cluster")
        results = []
        for result in response["aggregations"]["results"]["buckets"]:
            results.append(result["key"])
        yield results


def get_sum_and_count_aggregation_results(keyword):
    index_name = "{}-*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX)
    query = {
        "query": base_query(keyword),
        "aggs": {
            "prime_awards_obligation_amount": {"sum": {"field": "transaction_amount"}},
            "prime_awards_count": {"value_count": {"field": "transaction_id"}},
        },
        "size": 0,
    }
    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        try:
            results = {}
            results["prime_awards_count"] = response["aggregations"]["prime_awards_count"]["value"]
            results["prime_awards_obligation_amount"] = round(
                response["aggregations"]["prime_awards_obligation_amount"]["value"], 2
            )
            return results
        except KeyError:
            logger.exception("Unexpected Response")
    else:
        return None


def spending_by_transaction_sum_and_count(request_data):
    return get_sum_and_count_aggregation_results(request_data["filters"]["keywords"])


def concat_if_array(data):
    if isinstance(data, str):
        return data
    else:
        if isinstance(data, list):
            str_from_array = " ".join(data)
            return str_from_array
        else:
            # This should never happen if TinyShield is functioning properly
            logger.error("Keyword submitted was not a string or array")
            return ""


def get_number_of_unique_terms(filter_query: ES_Q, field: str) -> int:
    search = TransactionSearch().filter(filter_query)
    cardinality_aggregation = A("cardinality", field=field)
    search.aggs.metric("field_count", cardinality_aggregation)
    response = search.handle_execute()
    response_dict = response.aggs.to_dict()
    return response_dict.get("field_count", {"value": 0})["value"]


def get_sum_aggregations(field_to_sum: str, pagination: Pagination) -> Dict[str, A]:
    sum_as_cents = A("sum", field=field_to_sum, script={"source": "_value * 100"})
    sum_as_dollars = A(
        "bucket_script", buckets_path={"sum_as_cents": "sum_as_cents"}, script="params.sum_as_cents / 100"
    )

    # Have to create a separate dictionary for the bucket_sort values since "from" is a reserved word
    bucket_sort_values = {
        "sort": {"sum_as_dollars": {"order": "desc"}},
        "from": (pagination.page - 1) * pagination.limit,
        "size": pagination.limit + 1,
    }
    sum_bucket_sort = A("bucket_sort", **bucket_sort_values)

    return {"sum_as_cents": sum_as_cents, "sum_as_dollars": sum_as_dollars, "sum_bucket_sort": sum_bucket_sort}
