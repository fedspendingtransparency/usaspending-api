import logging

from django.conf import settings
from rest_framework.response import Response
from collections import OrderedDict

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.search.v2.elasticsearch_helper import es_sanitize
from usaspending_api.common.validator.tinyshield import validate_post_request
from usaspending_api.search.v2.elasticsearch_helper import es_sanitize


logger = logging.getLogger("console")

INDEX = "{}*".format(settings.TRANSACTIONS_INDEX_ROOT)

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
    endpoint_doc:
    """

    @cache_response()
    def post(self, request, format=None):

        search_text = es_sanitize(request.data["search_text"])
        country = es_sanitize(request.data["filter"]["country_code"])
        state = es_sanitize(request.data["filter"]["state_code"])
        scope = "recipient_location" if request.data["filter"]["scope"] == "recipient_location" else "pop"
        limit = request.data["limit"]
        return_fields = es_sanitize(["{}_city_name".format(scope), "{}_state_code".format(scope)])
        query_string = create_es_search("wildcard", scope, search_text, country, state)

        query = {
            "_source": return_fields,
            "size": 0,
            "query": query_string,
            "aggs": {
                "cities": {
                    "terms": {"field": "{}.keyword".format(return_fields[0]), 
                            "size": 50000,
                            "order": {
                                "max_score": "desc"
                            }},
                    "aggs": {
                        "max_score": {"max": {"script": "_score"}},
                        "states": {"terms": {"field": return_fields[1], "size": 50000}}
                        
                        },
                }
            }
        }

        hits = es_client_query(index=INDEX, body=query, retries=10)

        results = []
        if hits and hits["hits"]["total"] > 0:
            for city in hits["aggregations"]["cities"]["buckets"]:
                for state_code in city["states"]["buckets"]:
                    results.append(OrderedDict([("city_name", city["key"]), ("state_code", state_code["key"])]))

        #sorted_results = sorted(results, key=lambda x: (x["city_name"], x["state_code"]))

        response = OrderedDict([("count", len(results)), ("results", results[:limit])])
        return Response(response)


def create_es_search(method, scope, search_text, country=None, state=None):
    """
        Providing the parameters, create a value query sub-string for elasticsearch

        IF there is a need to perform a fuzzy search, there might be a need to set
            ["query_string"]["fuzzy_prefix_length"] to a value like 1
    """
    method_char = "~" if method == "fuzzy" else "*"
    if state:
        start_string = "({scope}_country_code:USA) AND ({scope}_state_code:{state}) AND"
        query_string = start_string.format(scope=scope, state=state)
    elif country and country != "USA":
        query_string = "({scope}_country_code:{country}) AND".format(scope=scope, country=country)
    else:
        query_string = "({scope}_country_code:USA) AND ({scope}_country_code:UNITED STATES) AND".format(scope=scope)

    query_string += '({scope}_city_name:{text}{char})'.format(scope=scope, text=search_text, char=method_char)

    query_string = es_sanitize(query_string)

    query = {"query_string": {"query": query_string, "allow_leading_wildcard": False}}
    if not state:
        # This produces better results for queries without a specified state.
        query['query_string']["analyzer"] = "keyword_analyzer"
    return query
