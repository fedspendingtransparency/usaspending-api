import logging

from django.conf import settings
from rest_framework.response import Response
from collections import OrderedDict

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.search.v2.elasticsearch_helper import preprocess
from usaspending_api.common.validator.tinyshield import validate_post_request


logger = logging.getLogger("console")

INDEX = "{}*".format(settings.TRANSACTIONS_INDEX_ROOT)

models = [
             {
                'name': 'filter|country_code',
                'key': 'filter|country_code',
                'type': 'text',
                'text_type': 'search',
                'optional': False
                }, {
                'key': 'filter|state_code',
                'name': 'fitler|state_code',
                'type': 'text',
                'text_type': 'search',
                'optional': True,
                'default': None,
                'allow_nulls': True
                }, {
                'key': 'filter|scope',
                'name': 'filter|scope',
                'type': 'enum',
                'enum_values': ("recipient_location", "primary_place_of_performance"),
                'optional': False
                }, {
                "key": "search_text",
                "name": "search_text",
                "type": "text",
                "text_type": "search",
                "optional": False,
                }, {
                "key": "method",
                "name": "method",
                "type": "enum",
                'enum_values': ('wildcard', 'fuzzy'),
                "optional": True,
                "default": "wildcard"
                }, {
                "key": "limit",
                "name": "limit",
                "type": "integer",
                "max": 500,
                "optional": True,
                "default": 10
                }
        ]


@validate_post_request(models)
class CityAutocompleteViewSet(APIDocumentationView):
    """
    endpoint_doc:
    """

    @cache_response()
    def post(self, request, format=None):

        search_text = preprocess(request.data["search_text"])
        country = request.data['filter']['country_code']
        state = request.data['filter']['state_code']
        scope = 'recipient_location' if request.data['filter']['scope'] == 'recipient_location' else 'pop'
        method = request.data['method']
        limit = request.data['limit']
        return_fields = ["{}_city_name".format(scope), "{}_state_code".format(scope)]
        if state:
            start_string = "({scope}_country_code:USA) AND ({scope}_state_code:{state}) AND"
            query_string = start_string.format(scope=scope, state=state)
        elif country and country != "USA":
            query_string = "({scope}_country_code:{country}) AND".format(scope=scope, country=country)
        else:
            query_string = "({scope}_country_code:USA) AND ({scope}_country_code:UNITED STATES) AND".format(scope=scope)

        method_char = "*" if method == "wildcard" else "~"

        query_string += "({scope}_city_name:{city_partial}{method_char})".format(scope=scope,
                                                                                 city_partial=search_text,
                                                                                 method_char=method_char)
        query = {
            "_source": return_fields,
            "size": 0,
            "query": {
                "query_string": {
                    "query": query_string
                }
            },
            "aggs": {
                "cities": {
                    "terms": {
                        "field": "{}.keyword".format(return_fields[0]),
                        "size": 50000
                    },
                    "aggs": {
                        "states": {
                            "terms": {
                                "field": return_fields[1],
                                "size": 50000
                            }
                        }
                    }
                }
            }
        }

        if method == "fuzzy":
            query["query"]["query_string"]["fuzzy_prefix_length"] = 1

        hits = es_client_query(index=INDEX, body=query, retries=10)
        results = []
        if hits:
            for city in hits["aggregations"]["cities"]["buckets"]:
                for state_code in city["states"]["buckets"]:
                    results.append({"state_code": state_code["key"], "city_name": city["key"]})

        sorted_results = sorted(results, key=lambda x: (x["city_name"], x["state_code"]))

        response = OrderedDict(
            [
                ("count", len(sorted_results[:limit])),
                ("results", sorted_results[:limit]),
            ]
        )
        return Response(response)
