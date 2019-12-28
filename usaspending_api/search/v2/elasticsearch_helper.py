import logging
import re

from django.conf import settings
from elasticsearch_dsl import Search, Q

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    TRANSACTIONS_LOOKUP,
    TRANSACTIONS_SOURCE_LOOKUP,
    KEYWORD_DATATYPE_FIELDS,
    INDEX_ALIASES_TO_AWARD_TYPES,
)
from usaspending_api.common.elasticsearch.client import es_client_query, es_dsl_search
from usaspending_api.common.exceptions import InvalidParameterException

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
        total = response["hits"]["total"]
        results = format_for_frontend(response["hits"]["hits"])
        return True, results, total
    else:
        return False, "There was an error connecting to the ElasticSearch cluster", None


def get_total_results(keyword, sub_index, retries=3):
    index_name = "{}-{}*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX, sub_index.replace("_", ""))
    query = {"query": base_query(keyword)}

    response = es_client_query(index=index_name, body=query, retries=retries)
    if response:
        try:
            return response["hits"]["total"]
        except KeyError:
            logger.error("Unexpected Response")
    else:
        logger.error("No Response")
        return None


def spending_by_transaction_count(request_data):
    keyword = request_data["filters"]["keywords"]
    response = {}

    for category in INDEX_ALIASES_TO_AWARD_TYPES.keys():
        total = get_total_results(keyword, category)
        if total is not None:
            if category == "directpayments":
                category = "direct_payments"
            response[category] = total
        else:
            return total
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


def get_base_search_with_filters(index_name: str, filters: dict) -> Search:
    search = es_dsl_search(index_name)
    key_list = [
        "keywords",
        "elasticsearch_keyword",
        "time_period",
        "award_type_codes",
        "agencies",
        "legal_entities",
        "recipient_id",
        "recipient_search_text",
        "recipient_scope",
        "recipient_locations",
        "recipient_type_names",
        "place_of_performance_scope",
        "place_of_performance_locations",
        "award_amounts",
        "award_ids",
        "program_numbers",
        "naics_codes",
        "psc_codes",
        "contract_pricing_type_codes",
        "set_aside_type_codes",
        "extent_competed_type_codes",
        "tas_codes",
    ]
    must_queries = []

    for key, value in filters.items():
        if value is None:
            raise InvalidParameterException(f"Invalid filter: {key} has null as its value.")

        if key not in key_list:
            raise InvalidParameterException(f"Invalid filter: {key} does not exist.")

        if key == "keywords":
            keyword_queries = []
            fields = [
                "recipient_name",
                "naics_description",
                "product_or_service_description",
                "award_description",
                "piid",
                "fain",
                "uri",
                "recipient_unique_id",
                "parent_recipient_unique_id",
            ]
            for v in value:
                keyword_queries.append(Q("query_string", query=v, default_operator="AND", fields=fields))

            must_queries.append(Q("dis_max", queries=keyword_queries))

        elif key == "time_period":
            time_period_query = []

            for v in value:
                start_date = v.get("start_date") or settings.API_SEARCH_MIN_DATE
                end_date = v.get("end_date") or settings.API_MAX_DATE
                time_period_query.append(Q("range", action_date={"gte": start_date, "lte": end_date}))

            must_queries.append(Q("bool", should=time_period_query, minimum_should_match=1))

        elif key == "award_type_codes":
            award_type_codes_query = []

            for v in value:
                award_type_codes_query.append(Q("match", type=v))

            must_queries.append(Q("bool", should=award_type_codes_query, minimum_should_match=1))

        elif key == "agencies":
            awarding_agency_query = []
            funding_agency_query = []

            agency_query_lookup = {
                "awarding": {
                    "toptier": lambda name: awarding_agency_query.append(
                        Q("match", awarding_toptier_agency_name__keyword=name)
                    ),
                    "subtier": lambda name: awarding_agency_query.append(
                        Q("match", awarding_subtier_agency_name__keyword=name)
                    ),
                },
                "funding": {
                    "toptier": lambda name: funding_agency_query.append(
                        Q("match", funding_toptier_agency_name__keyword=name)
                    ),
                    "subtier": lambda name: funding_agency_query.append(
                        Q("match", funding_subtier_agency_name__keyword=name)
                    ),
                },
            }

            for v in value:
                agency_name = v["name"]
                agency_tier = v["tier"]
                agency_type = v["type"]
                agency_query = Q("match", **{f"{agency_type}_{agency_tier}_agency_name__keyword": agency_name})
                if agency_type == "awarding":
                    awarding_agency_query.append(agency_query)
                elif agency_type == "funding":
                    funding_agency_query.append(agency_query)

            must_queries.extend(
                [
                    Q("bool", should=awarding_agency_query, minimum_should_match=1),
                    Q("bool", should=funding_agency_query, minimum_should_match=1),
                ]
            )

        elif key == "legal_entities":
            # This filter key has effectively become obsolete by recipient_search_text
            msg = 'API request included "{}" key. No filtering will occur with provided value "{}"'
            logger.info(msg.format(key, value))

        elif key == "recipient_search_text":
            recipient_search_query = []
            fields = ["recipient_name"]

            for v in value:
                upper_recipient_string = v.upper()
                recipient_name_query = Q(
                    "query_string", query=upper_recipient_string, default_operator="AND", fields=fields
                )

                if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                    recipient_duns_query = Q("match", recipient_unique_id=upper_recipient_string)
                    recipient_search_query.append(Q("dis_max", queries=[recipient_name_query, recipient_duns_query]))
                else:
                    recipient_search_query.append(recipient_name_query)

            must_queries.append(Q("bool", should=recipient_search_query, minimum_should_match=1))

        elif key == "recipient_id":

            if value.endswith("P"):
                recipient_id_query = Q("match", parent_recipient_hash=value)
            else:
                recipient_id_query = Q("match", recipient_hash=value)

            must_queries.append(Q("bool", must=recipient_id_query))

        elif key == "recipient_scope":
            recipient_scope_query = Q("match", recipient_location_country_code="USA")

            if value == "domestic":
                must_queries.append(Q("bool", must=recipient_scope_query))
            elif value == "foreign":
                must_queries.append(Q("bool", must_not=recipient_scope_query))

        elif key == "recipient_locations":
            recipient_locations_query = []

            for v in value:
                location_query = []
                location_lookup = {
                    "country_code": v.get("country"),
                    "state_code": v.get("state"),
                    "county_code": v.get("county"),
                    "congressional_code": v.get("district"),
                    "city_name__keyword": v.get("city"),
                    "zip5": v.get("zip"),
                }

                for location_key, location_value in location_lookup.items():
                    if location_value is not None:
                        location_query.append(Q("match", **{f"recipient_location_{location_key}": location_value}))

                recipient_locations_query.append(Q("bool", must=location_query))

            must_queries.append(Q("bool", should=recipient_locations_query, minimum_should_match=1))

        elif key == "recipient_type_names":
            recipient_type_query = []

            for v in value:
                recipient_type_query.append(Q("match", business_categories=v))

            must_queries.append(Q("bool", should=recipient_type_query, minimum_should_match=1))

        elif key == "place_of_performance_scope":
            pop_scope_query = Q("match", pop_country_code="USA")

            if value == "domestic":
                must_queries.append(Q("bool", must=pop_scope_query))
            elif value == "foreign":
                must_queries.append(Q("bool", must_not=pop_scope_query))

        elif key == "place_of_performance_locations":
            pop_locations_query = []

            for v in value:
                location_query = []
                location_lookup = {
                    "country_code": v.get("country"),
                    "state_code": v.get("state"),
                    "county_code": v.get("county"),
                    "congressional_code": v.get("district"),
                    "city_name__keyword": v.get("city"),
                    "zip5": v.get("zip"),
                }

                for location_key, location_value in location_lookup.items():
                    if location_value is not None:
                        location_query.append(Q("match", **{f"pop_{location_key}": location_value}))

                pop_locations_query.append(Q("bool", must=location_query))

            must_queries.append(Q("bool", should=pop_locations_query, minimum_should_match=1))

        elif key == "award_amounts":
            award_amounts_query = []

            for v in value:
                lower_bound = v.get("lower_bound")
                upper_bound = v.get("upper_bound")
                award_amounts_query.append(Q("range", award_amount={"gte": lower_bound, "lte": upper_bound}))

            must_queries.append(Q("bool", should=award_amounts_query, minimum_should_match=1))

        elif key == "award_ids":
            award_ids_query = []

            for v in value:
                award_ids_query.append(Q("match", display_award_id=v))

            must_queries.append(Q("bool", should=award_ids_query, minimum_should_match=1))

        elif key == "program_numbers":
            programs_numbers_query = []

            for v in value:
                programs_numbers_query.append(Q("match", cfda_number=v))

            must_queries.append(Q("bool", should=programs_numbers_query, minimum_should_match=1))

        elif key == "naics_codes":
            naics_codes_query = []

            for v in value:
                naics_codes_query.append(Q("match", naics_code__keyword=v))

            must_queries.append(Q("bool", should=naics_codes_query, minimum_should_match=1))

        elif key == "psc_codes":
            psc_codes_query = []

            for v in value:
                psc_codes_query.append(Q("match", product_or_service_code__keyword=v))

            must_queries.append(Q("bool", should=psc_codes_query, minimum_should_match=1))

        elif key == "contract_pricing_type_codes":
            contract_pricing_query = []

            for v in value:
                contract_pricing_query.append(Q("match", type_of_contract_pricing__keyword=v))

            must_queries.append(Q("bool", should=contract_pricing_query, minimum_should_match=1))

        elif key == "set_aside_type_codes":
            set_aside_query = []

            for v in value:
                set_aside_query.append(Q("match", type_set_aside__keyword=v))

            must_queries.append(Q("bool", should=set_aside_query, minimum_should_match=1))

        elif key == "extent_competed_type_codes":
            extent_competed_query = []

            for v in value:
                extent_competed_query.append(Q("match", extent_competed__keyword=v))

            must_queries.append(Q("bool", should=extent_competed_query, minimum_should_match=1))

        elif key == "tas_codes":
            tas_codes_query = []

            for v in value:
                code_query = []
                code_lookup = {
                    "aid": v.get("aid"),
                    "ata": v.get("ata"),
                    "main": v.get("main"),
                    "sub": v.get("sub"),
                    "bpoa": v.get("bpoa"),
                    "epoa": v.get("epoa"),
                    "a": v.get("a"),
                }

                for code_key, code_value in code_lookup.items():
                    if code_value is not None:
                        code_query.append(Q("match", **{f"treasury_accounts__{code_key}": code_value}))

                tas_codes_query.append(Q("bool", must=code_query))

            nested_query = Q("bool", should=tas_codes_query, minimum_should_match=1)
            must_queries.append(Q("nested", path="treasury_accounts", query=nested_query))

    return search.filter("bool", must=must_queries)
