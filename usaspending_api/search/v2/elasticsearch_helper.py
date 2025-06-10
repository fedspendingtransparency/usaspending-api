import logging
from decimal import Decimal
from typing import Dict, Optional

from django.conf import settings
from elasticsearch_dsl import A
from elasticsearch_dsl import Q as ES_Q

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    INDEX_ALIASES_TO_AWARD_TYPES,
    TRANSACTIONS_SOURCE_LOOKUP,
)
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, Search, TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.v2.es_sanitization import es_minimal_sanitize
from usaspending_api.search.filters.elasticsearch.filter import QueryType

logger = logging.getLogger("console")

DOWNLOAD_QUERY_SIZE = settings.MAX_DOWNLOAD_LIMIT
TRANSACTIONS_SOURCE_LOOKUP.update({v: k for k, v in TRANSACTIONS_SOURCE_LOOKUP.items()})


def swap_keys(dictionary_):
    return dict(
        (TRANSACTIONS_SOURCE_LOOKUP.get(old_key, old_key), new_key) for (old_key, new_key) in dictionary_.items()
    )


def format_for_frontend(response):
    """calls reverse key from TRANSACTIONS_LOOKUP"""
    response = [result["_source"] for result in response]
    return [swap_keys(result) for result in response]


def get_total_results(keyword):
    group_by_agg_key_values = {
        "filters": {category: {"terms": {"type": types}} for category, types in INDEX_ALIASES_TO_AWARD_TYPES.items()}
    }
    aggs = A("filters", **group_by_agg_key_values)
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    filter_query = query_with_filters.generate_elasticsearch_query({"keyword_search": [es_minimal_sanitize(keyword)]})
    search = TransactionSearch().filter(filter_query)
    search.aggs.bucket("types", aggs)
    response = search.handle_execute()

    if response is not None:
        try:
            return response["aggregations"]["types"]["buckets"]
        except KeyError:
            logger.error("Unexpected Response")
    else:
        logger.error("No Response")
        return None


def spending_by_transaction_count(search_query):
    group_by_agg_key_values = {
        "filters": {category: {"terms": {"type": types}} for category, types in INDEX_ALIASES_TO_AWARD_TYPES.items()}
    }
    aggs = A("filters", **group_by_agg_key_values)
    search_query.aggs.bucket("types", aggs)
    query_response = search_query.handle_execute()

    results = None
    if query_response is not None:
        results = query_response["aggregations"]["types"]["buckets"]

    if results is None:
        return None

    response = {}
    for category in INDEX_ALIASES_TO_AWARD_TYPES.keys():
        if category == "directpayments":
            response["direct_payments"] = results[category]["doc_count"]
        else:
            response[category] = results[category]["doc_count"]
    return response


def get_sum_aggregation_results(keyword, field="federal_action_obligation"):
    group_by_agg_key_values = {"field": field}
    aggs = A("sum", **group_by_agg_key_values)
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    filter_query = query_with_filters.generate_elasticsearch_query({"keyword_search": [es_minimal_sanitize(keyword)]})
    search = TransactionSearch().filter(filter_query)
    search.aggs.bucket("transaction_sum", aggs)
    response = search.handle_execute()

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
    n_iter = DOWNLOAD_QUERY_SIZE // size

    results = get_total_results(keyword)
    if results is None:
        logger.error("Error retrieving total results. Max number of attempts reached")
        return
    total = sum(results[category]["doc_count"] for category in INDEX_ALIASES_TO_AWARD_TYPES.keys())
    required_iter = (total // size) + 1
    n_iter = min(max(1, required_iter), n_iter)
    for i in range(n_iter):
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(
            {"keyword_search": [es_minimal_sanitize(keyword)]}
        )
        search = TransactionSearch().filter(filter_query)
        group_by_agg_key_values = {
            "field": field,
            "include": {"partition": i, "num_partitions": n_iter},
            "size": size,
            "shard_size": size,
        }
        aggs = A("terms", **group_by_agg_key_values)
        search.aggs.bucket("results", aggs)
        response = search.handle_execute()
        if response is None:
            raise Exception("Breaking generator, unable to reach cluster")
        results = []
        for result in response["aggregations"]["results"]["buckets"]:
            results.append(result["key"])
        yield results


def get_sum_and_count_aggregation_results(keyword):
    query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
    filter_query = query_with_filters.generate_elasticsearch_query({"keyword_search": [es_minimal_sanitize(keyword)]})
    search = TransactionSearch().filter(filter_query)
    search.aggs.bucket("prime_awards_obligation_amount", {"sum": {"field": "federal_action_obligation"}})
    search.aggs.bucket("prime_awards_count", {"value_count": {"field": "transaction_id"}})
    response = search.handle_execute()

    if response is not None:
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


def get_number_of_unique_terms_for_transactions(filter_query: ES_Q, field: str) -> int:
    """
    Returns the count for a specific filter_query.
    NOTE: Counts below the precision_threshold are expected to be close to accurate (per the Elasticsearch
          documentation). Since aggregations do not support more than 10k buckets this value is hard coded to
          11k to ensure that endpoints using Elasticsearch do not cross the 10k threshold. Elasticsearch endpoints
          should be implemented with a safeguard in case this count is above 10k.
    """
    # TODO: Should update references to this function to use "get_number_of_unique_terms" directly
    return get_number_of_unique_terms(TransactionSearch, filter_query, field)


def get_number_of_unique_terms_for_awards(filter_query: ES_Q, field: str) -> int:
    """
    Returns the count for a specific filter_query.
    NOTE: Counts below the precision_threshold are expected to be close to accurate (per the Elasticsearch
          documentation). Since aggregations do not support more than 10k buckets this value is hard coded to
          11k to ensure that endpoints using Elasticsearch do not cross the 10k threshold. Elasticsearch endpoints
          should be implemented with a safeguard in case this count is above 10k.
    """
    # TODO: Should update references to this function to use "get_number_of_unique_terms" directly
    return get_number_of_unique_terms(AwardSearch, filter_query, field)


def get_number_of_unique_terms(
    search_type: type[Search], query: ES_Q, field: str, nested_path: str | None = None
) -> int:
    """
    Returns the count for a specific filter_query.
    NOTE: Counts below the precision_threshold are expected to be close to accurate (per the Elasticsearch
          documentation). Since aggregations do not support more than 10k buckets this value is hard coded to
          11k to ensure that endpoints using Elasticsearch do not cross the 10k threshold. Elasticsearch endpoints
          should be implemented with a safeguard in case this count is above 10k.
    """
    search = search_type().filter(query)
    cardinality_aggregation = A("cardinality", field=field, precision_threshold=11000)
    if nested_path:
        search.aggs.bucket("nested_agg", A("nested", path=nested_path))
        search.aggs["nested_agg"].metric("field_count", cardinality_aggregation)
    else:
        search.aggs.metric("field_count", cardinality_aggregation)
    response = search.handle_execute()
    response_dict = response.aggs.to_dict()
    if nested_path:
        response_dict = response_dict["nested_agg"]

    return response_dict.get("field_count", {"value": 0})["value"]


def get_scaled_sum_aggregations(field_to_sum: str, pagination: Optional[Pagination] = None) -> Dict[str, A]:
    """
    Creates a sum and bucket_sort aggregation that can be used for many different aggregations.
    The sum aggregation scaled the values by 100 so that the scaled_floats are handled as integers to avoid
    issues surrounding floats. This does mean that after retrieving results from Elasticsearch something
    similar to the code below is needed to convert to two decimal places.

        Example:
        Decimal(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100")

    """
    sum_field = A("sum", field=field_to_sum, script={"source": "_value * 100"})

    if pagination:
        # Have to create a separate dictionary for the bucket_sort values since "from" is a reserved word
        bucket_sort_values = {
            "from": (pagination.page - 1) * pagination.limit,
            "size": pagination.limit + 1,
        }

        # Two different bucket sort aggregations to choose from:
        # sum_bucket_sort -> used when parent aggregation is not sorting
        # sum_bucket_truncate -> used when parent aggregation is sorting and only a specific page is needed
        sum_bucket_sort = A("bucket_sort", sort={"sum_field": {"order": "desc"}}, **bucket_sort_values)
        sum_bucket_truncate = A("bucket_sort", **bucket_sort_values)
        return {"sum_field": sum_field, "sum_bucket_sort": sum_bucket_sort, "sum_bucket_truncate": sum_bucket_truncate}
    else:
        return {"sum_field": sum_field}


def get_summed_value_as_float(bucket: dict, field: str) -> Decimal:
    """
    Elasticsearch commonly has problems handling the sum of floating point numbers (even if they are stored as
    "Scaled Float" type). Due to that the Elasticsearch sum aggregations handle the Scaled Floats as integers
    and then our API converts those integers to a float with up to two decimal places.
    """
    value = bucket.get(field, {"value": 0})["value"]
    return int(value) / Decimal("100")
