from django.conf import settings
from rest_framework.response import Response
from collections import OrderedDict

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.search.v2.elasticsearch_helper import es_sanitize
from usaspending_api.common.validator.tinyshield import validate_post_request


models = [
    {
        "name": "filter|country_code",
        "key": "filter|country_code",
        "type": "text",
        "text_type": "search",
        "optional": False,
    },
    {
        "key": "filter|state_code",
        "name": "fitler|state_code",
        "type": "text",
        "text_type": "search",
        "optional": True,
        "default": None,
        "allow_nulls": True,
    },
    {
        "key": "filter|scope",
        "name": "filter|scope",
        "type": "enum",
        "enum_values": ("recipient_location", "primary_place_of_performance"),
        "optional": False,
    },
    {"key": "search_text", "name": "search_text", "type": "text", "text_type": "search", "optional": False},
    {"key": "limit", "name": "limit", "type": "integer", "max": 500, "optional": True, "default": 10},
]


@validate_post_request(models)
class CityAutocompleteViewSet(APIDocumentationView):
    """
    endpoint_doc: usaspending_api/api_contracts/autocomplete/City.md
    """

    @cache_response()
    def post(self, request, format=None):
        search_text, country, state = prepare_search_terms(request.data)
        scope = "recipient_location" if request.data["filter"]["scope"] == "recipient_location" else "pop"
        limit = request.data["limit"]
        return_fields = ["{}_city_name".format(scope), "{}_state_code".format(scope)]

        query = create_elasticsearch_query(return_fields, scope, search_text, country, state, limit)
        sorted_results = query_elasticsearch(query)
        response = OrderedDict([("count", len(sorted_results)), ("results", sorted_results[:limit])])

        return Response(response)


def prepare_search_terms(request_data):
    fields = [request_data["search_text"], request_data["filter"]["country_code"], request_data["filter"]["state_code"]]

    return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]


def create_elasticsearch_query(return_fields, scope, search_text, country, state, limit):
    query_string = create_es_search(scope, search_text, country, state)
    # Buffering the "group by city" to create a handful more buckets, more than the number of shards we expect to have,
    # so that we don't get inconsistent results when the limit gets down to a very low number (e.g. lower than the
    # number of shards we have) such that it may provide inconsistent results in repeated queries
    city_buckets = limit + 100
    query = {
        "_source": return_fields,
        "size": 0,
        "query": {
            "bool": {
                "filter": {
                    "bool": query_string
                }
            }
        },
        "aggs": {
            "cities": {
                "terms": {
                    "field": "{}.keyword".format(return_fields[0]),
                    "size": city_buckets,
                },
                "aggs": {
                    "states": {"terms": {"field": return_fields[1], "size": 100}}
                },
            }
        }
    }
    return query


def create_es_search(scope, search_text, country=None, state=None):
    """
        Providing the parameters, create a dictionary representing the bool-query conditional clauses for elasticsearch

        Args:
            scope: which city field was chosen for searching `pop` (place of performance) or `recipient_location`
            search_text: the text the user is typing in and sent to the backend
            country: optional country selected by user
            state: optional state selected by user
    """
    # The base query that will do a wildcard term-level query
    query = {
        "must": [
            {
                "wildcard": {
                    "{}_city_name.keyword".format(scope): search_text + "*"
                }
            }
        ]
    }

    if state:
        # States are only supported for Country=USA
        query["must"].append({"match": {"{scope}_state_code".format(scope=scope): state}})
        query["should"] = [build_country_match(scope, "USA"), build_country_match(scope, "UNITED STATES")]
        query["minimum_should_match"] = 1
    elif country == "FOREIGN":
        # Create a "Should Not" query with a nested bool, to get everything non-USA
        query["should"] = {
            "bool": {
                "must_not": [build_country_match(scope, "USA"), build_country_match(scope, "UNITED STATES")]
            }
        }
    elif country != "USA":
        # A non-USA selected country
        query["must"].append({"match": {"{scope}_country_code".format(scope=scope): country}})
    else:
        # USA is selected as country
        query["should"] = [build_country_match(scope, "USA"), build_country_match(scope, "UNITED STATES")]
        query["minimum_should_match"] = 1

    return query


def build_country_match(country_match_scope, country_match_country):
    country_match = {
        "match": {
            "{}_country_code".format(country_match_scope): country_match_country
        }
    }
    return country_match


def query_elasticsearch(query):
    hits = es_client_query(index="{}*".format(settings.TRANSACTIONS_INDEX_ROOT), body=query)

    results = []
    if hits and hits["hits"]["total"] > 0:
        results = parse_elasticsearch_response(hits)
        results = sorted(results, key=lambda x: (x["city_name"], x["state_code"]))
    return results


def parse_elasticsearch_response(hits):
    results = []
    for city in hits["aggregations"]["cities"]["buckets"]:
        if len(city["states"]["buckets"]) > 0:
            for state_code in city["states"]["buckets"]:
                results.append(OrderedDict([("city_name", city["key"]), ("state_code", state_code["key"])]))
        else:
            # for cities without states, useful for foreign country results
            results.append(OrderedDict([("city_name", city["key"]), ("state_code", None)]))
    return results
