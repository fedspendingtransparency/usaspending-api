from rest_framework.response import Response
from rest_framework.views import APIView
from collections import OrderedDict
from elasticsearch_dsl import Q as ES_Q, A

from usaspending_api.common.cache_decorator import cache_response

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.search.v2.es_sanitization import es_sanitize
from usaspending_api.common.validator.tinyshield import validate_post_request
from usaspending_api.awards.v2.filters.location_filter_geocode import ALL_FOREIGN_COUNTRIES


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
class CityAutocompleteViewSet(APIView):
    """
    This end point returns a list of cities for a given limit, country, search string, and optional state code.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/city.md"

    @cache_response()
    def post(self, request, format=None):
        search_text, country, state = prepare_search_terms(request.data)
        scope = "recipient_location" if request.data["filter"]["scope"] == "recipient_location" else "pop"
        limit = request.data["limit"]
        return_fields = ["{}_city_name".format(scope), "{}_state_code".format(scope), "{}_country_code".format(scope)]

        query = create_elasticsearch_query(return_fields, scope, search_text, country, state, limit)
        sorted_results = query_elasticsearch(query)
        response = OrderedDict([("count", len(sorted_results)), ("results", sorted_results[:limit])])

        return Response(response)


def prepare_search_terms(request_data):
    fields = [request_data["search_text"], request_data["filter"]["country_code"], request_data["filter"]["state_code"]]

    return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]


def create_elasticsearch_query(return_fields, scope, search_text, country, state, limit):
    query = create_es_search(scope, search_text, country, state)
    # Buffering the "group by city" to create a handful more buckets, more than the number of shards we expect to have,
    # so that we don't get inconsistent results when the limit gets down to a very low number (e.g. lower than the
    # number of shards we have) such that it may provide inconsistent results in repeated queries
    city_buckets = limit + 100
    if country != "USA":
        aggs = {"field": return_fields[2], "size": 100}
    else:
        aggs = {"field": return_fields[1], "size": 100}
    agg_values = {"field": "{}.keyword".format(return_fields[0]), "size": city_buckets}
    agg_key = A("terms", **agg_values)
    query.aggs.bucket("cities", agg_key).bucket("states", A("terms", **aggs))
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
    query = {"must": [{"wildcard": {"{}_city_name.keyword".format(scope): search_text + "*"}}]}
    if country != "USA":
        # A non-USA selected country
        if country != ALL_FOREIGN_COUNTRIES:
            query["must"].append({"match": {"{scope}_country_code".format(scope=scope): country}})
        # Create a "Should Not" query with a nested bool, to get everything non-USA
        query["should"] = [
            {
                "bool": {
                    "must": {"exists": {"field": "{}_country_code".format(scope)}},
                    "must_not": [
                        {"match": {"{}_country_code".format(scope): "USA"}},
                        {"match_phrase": {"{}_country_code".format(scope): "UNITED STATES"}},
                    ],
                }
            }
        ]
        query["minimum_should_match"] = 1
    else:
        # USA is selected as country
        query["should"] = [build_country_match(scope, "USA"), build_country_match(scope, "UNITED STATES")]
        query["should"].append({"bool": {"must_not": {"exists": {"field": "{}_country_code".format(scope)}}}})
        query["minimum_should_match"] = 1
        # null country codes are being considered as USA country codes

    if state:
        # If a state was provided, include it in the filter to limit hits
        query["must"].append({"match": {"{}_state_code".format(scope): es_sanitize(state).upper()}})

    search = TransactionSearch().filter(ES_Q("bool", **query))
    return search


def build_country_match(country_match_scope, country_match_country):
    country_match = {"match": {"{}_country_code".format(country_match_scope): country_match_country}}
    return country_match


def query_elasticsearch(query):
    hits = query.handle_execute()
    results = []
    if hits and hits["hits"]["total"]["value"] > 0:
        results = parse_elasticsearch_response(hits)
        results = sorted(results, key=lambda x: x.pop("hits"), reverse=True)
    return results


def parse_elasticsearch_response(hits):
    results = []
    for city in hits["aggregations"]["cities"]["buckets"]:
        if len(city["states"]["buckets"]) > 0:
            for state_code in city["states"]["buckets"]:
                results.append(
                    OrderedDict(
                        [
                            ("city_name", city["key"]),
                            ("state_code", state_code["key"]),
                            ("hits", state_code["doc_count"]),
                        ]
                    )
                )
        else:
            # for cities without states, useful for foreign country results
            results.append(OrderedDict([("city_name", city["key"]), ("state_code", None), ("hits", city["doc_count"])]))
    return results
