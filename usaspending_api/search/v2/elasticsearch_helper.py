import logging
import re

from django.conf import settings
from django.db.models import Q

from usaspending_api.accounts.helpers import TAS_COMPONENT_TO_FIELD_MAPPING
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import KEYWORD_DATATYPE_FIELDS
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP, AWARDS_LOOKUP
from usaspending_api.common.elasticsearch.client import es_client_query, es_client_count
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger("console")

DOWNLOAD_QUERY_SIZE = settings.MAX_DOWNLOAD_LIMIT
KEYWORD_DATATYPE_FIELDS = ["{}.raw".format(i) for i in KEYWORD_DATATYPE_FIELDS]

TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})
AWARDS_LOOKUP.update({v: k for k, v in AWARDS_LOOKUP.items()})


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


def swap_keys(dictionary_, awards):
    lookup = TRANSACTIONS_LOOKUP
    if awards:
        lookup = AWARDS_LOOKUP
    return dict((lookup.get(old_key, old_key), new_key) for (old_key, new_key) in dictionary_.items())


def format_for_frontend(response, awards=False):
    """ calls reverse key from TRANSACTIONS_LOOKUP """
    response = [result["_source"] for result in response]
    return [swap_keys(result, awards) for result in response]


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
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in request_data["fields"]]
    query_fields.extend(["award_id"])
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


def base_awards_query(filters):
    query = {"bool": {"filter": {"bool": {"should": []}}}}
    for key, value in filters.items():
        if value is None:
            raise InvalidParameterException("Invalid filter: " + key + " has null as its value.")

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
            "elasticsearch",  # elasticsearch doesn't do anything but it's here because i put it there and now it's too late to change it
        ]

        if key not in key_list:
            raise InvalidParameterException("Invalid filter: " + key + " does not exist.")

        if key == "keywords":
            queries = []
            for v in value:
                x = v.split()
                y = " AND "
                if len(x) > 1:
                    z = y.join(x)
                else:
                    z = v
                queries.append({"query_string": {"query": z}})

            query["bool"]["filter"]["bool"]["should"] = query["bool"]["filter"]["bool"]["should"] + [{"dis_max": {"queries": queries}}]

        elif key == "time_period":
            should = []
            for v in value:
                should.append(
                    {
                        "bool": {
                            "should": [
                                {"range": {"action_date": {"gte": v["start_date"]}}},
                                {"range": {"date_signed": {"lte": v["end_date"]}}},
                            ],
                            "minimum_should_match": 2,
                        }
                    }
                )
            query["bool"].update({"should": should, "minimum_should_match": 1})

        elif key == "award_type_codes":
            should = []
            for v in value:
                should.append({"match": {"type": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0)
                                            + 1,
                }
            )

        elif key == "agencies":
            funding = False
            awarding = False
            should = []
            for v in value:
                if v["type"] == "funding":
                    funding = True
                else:
                    awarding = True
                field = "{}_{}_agency_name.keyword".format(v["type"], v["tier"])
                should.append({"match": {field: v["name"]}})
            min_match = 1
            if funding and awarding:
                min_match = 2
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0)
                    + min_match,
                }
            )

        # elif key == "legal_entities":

        elif key == "recipient_search_text":
            should = []
            for v in value:
                should.append({"wildcard": {"recipient_name": "{}*".format(v)}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        # elif key == "recipient_id":
        #
        elif key == "recipient_scope":
            should = []
            for v in value:
                should.append({"match": {"recipient_location_country_code": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "recipient_locations":
            should_outer = []
            for v in value:
                should = []
                locations = {
                    "country_code": v.get("country"),
                    "state_code": v.get("state"),
                    "county_code": v.get("county"),
                    "congressional_code": v.get("district"),
                    "city_name": v.get("city"),
                }
                min_match = 0
                for location in locations.keys():
                    if locations[location] is not None:
                        min_match += 1
                        should.append({"match": {"recipient_location_{}".format(location): locations[location]}})
                should_outer.append({"bool": {"should": should, "minimum_should_match": min_match}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should_outer,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "recipient_type_names":
            should = []
            for v in value:
                should.append({"wildcard": {"business_categories": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "place_of_performance_scope":
            should = []
            for v in value:
                should.append({"match": {"pop_country_code": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "place_of_performance_locations":
            should_outer = []
            for v in value:
                should = []
                locations = {
                    "country_code": v.get("country"),
                    "state_code": v.get("state"),
                    "county_code": v.get("county"),
                    "congressional_code": v.get("district"),
                    "city_name": v.get("city"),
                }
                min_match = 0
                for location in locations.keys():
                    if locations[location] is not None:
                        min_match += 1
                        should.append({"match": {"pop_{}".format(location): locations[location]}})
                should_outer.append({"bool": {"should": should, "minimum_should_match": min_match}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should_outer,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "award_amounts":
            should = []
            for v in value:
                lte = v.get("upper_bound")
                gte = v.get("lower_bound")
                should.append({"range": {"total_obligation": {"gte": gte, "lt": lte}}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "award_ids":
            should = []
            for v in value:
                should.append({"match": {"piid.keyword": v}})
                should.append({"match": {"fain.keyword": v}})
                should.append({"match": {"uri.keyword": v}})

            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "program_numbers":
            should = []
            for v in value:
                should.append({"match": {"cfda_number.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "naics_codes":
            should = []
            for v in value:
                if len(v) < 6:
                    should.append({"wildcard": {"naics_code.keyword": "{}*".format(v)}})
                else:
                    should.append({"match": {"naics_code.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "psc_codes":
            should = []
            for v in value:
                should.append({"match": {"product_or_service_code.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "contract_pricing_type_codes":
            should = []
            for v in value:
                should.append({"match": {"type_of_contract_pricing.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "set_aside_type_codes":
            should = []
            for v in value:
                should.append({"match": {"product_or_service_code.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "extent_competed_type_codes":
            should = []
            for v in value:
                should.append({"match": {"extent_completed.keyword": v}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

        elif key == "tas_codes":
            should = []
            for v in value:
                tas_qs = Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: x for k, x in v.items()})
                tas_ids = TreasuryAppropriationAccount.objects.filter(tas_qs).values_list(
                    "treasury_account_identifier", flat=True
                )
                for id in tas_ids:
                    should.append({"wildcard": {"treasury_account_identifiers": id}})
            query["bool"]["filter"]["bool"].update(
                {
                    "should": query["bool"]["filter"]["bool"]["should"] + should,
                    "minimum_should_match": int(query["bool"]["filter"]["bool"].get("minimum_should_match") or 0) + 1,
                }
            )

    # i am sorry about this
    if len(query["bool"]["filter"]["bool"]["should"]) == None:
        query["bool"]["filter"]["bool"].pop("should")
        if query["bool"]["filter"]["bool"] == {}:
            query["bool"]["filter"].pop("bool")
            if query["bool"]["filter"] == {}:
                query["bool"].pop("filter")
                if query["bool"] == {}:
                    query.pop("bool")
    return query


def search_awards(request_data, lower_limit, limit):
    """
    request_data: dictionary
    lower_limit: integer
    limit: integer
    if transaction_type_code not found, return results for contracts
    """

    filters = request_data["filters"]
    query_fields = [AWARDS_LOOKUP[i] for i in request_data["fields"]]
    query_fields.extend(["award_id"])
    query_fields.extend(["generated_unique_award_id"])
    query_fields.extend(["prime_award_recipient_id"])
    query_sort = AWARDS_LOOKUP[request_data["sort"]]
    query = {
        "_source": query_fields,
        "from": lower_limit,
        "size": limit,
        "query": base_awards_query(filters),
        "sort": [{query_sort: {"order": request_data["order"]}}],
    }
    print(query)
    for index, award_types in INDEX_ALIASES_TO_AWARD_TYPES.items():
        if sorted(award_types) == sorted(request_data["filters"]["award_type_codes"]):
            index_name = "{}-{}".format(settings.AWARDS_INDEX_ROOT, index)
        else:
            if set(request_data["filters"]["award_type_codes"]).issubset(award_types):
                index_name = "{}-{}".format(settings.AWARDS_INDEX_ROOT, index)

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        total = response["hits"]["total"]
        results = format_for_frontend(response["hits"]["hits"], True)
        return True, results, total
    else:
        return False, "There was an error connecting to the ElasticSearch cluster", 0


def elastic_awards_count(request_data):
    """
    request_data: dictionary
    lower_limit: integer
    limit: integer
    if transaction_type_code not found, return results for contracts
    """

    filters = request_data["filters"]
    query = {"query": base_awards_query(filters)}
    types = ["contracts", "idvs", "grants", "directpayments", "loans", "other"]
    response = {}
    success = True
    for t in types:
        index_name = "future-awards-{}".format(t)
        results = es_client_count(index=index_name, body=query, retries=10)
        if t == "directpayments":
            t = "direct_payments"
        if results:
            response.update({t: results["count"]})
        else:
            success = False
    if success:
        return response
    else:
        return "There was an error connecting to the ElasticSearch cluster"


def elasticsearch_dollar_sum_aggregation(column_to_sum):
    return {
        "sum_as_cents": {
            "sum": {"script": {"lang": "painless", "source": "doc['{}'].value * 100".format(column_to_sum)}}
        },
        "sum_as_dollars": {
            "bucket_script": {"buckets_path": {"sum_as_cents": "sum_as_cents"}, "script": "params.sum_as_cents / 100"}
        },
    }
