import logging

from django.conf import settings
from rest_framework.response import Response
from collections import OrderedDict

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.views import APIDocumentationView

from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.search.v2.elasticsearch_helper import es_sanitizer


logger = logging.getLogger("console")

INDEX = "{}*".format(settings.TRANSACTIONS_INDEX_ROOT)


class CityAutocompleteViewSet(APIDocumentationView):
    """
    endpoint_doc:
    """

    @cache_response()
    def get(self, request, format=None):
        search_text = request.GET.get("search_text")
        search_text = es_sanitizer(search_text)

        country = request.GET.get("country", None)
        state = request.GET.get("state", None)

        scope = request.GET.get("scope", "recipient_location")  # recipient_location, pop
        method = request.GET.get("method", "wildcard")

        if state:
            query_string = ("({scope}_country_code:USA)",
                            "AND ({scope}_state_code:{state}) AND").format(scope=scope, state=state)
        elif country and country != "USA":
            query_string = "({scope}_country_code:{country}) AND".format(scope=scope, country=country)
        else:
            query_string = "({scope}_country_code:USA) AND ({scope}_country_code:UNITED STATES) AND".format(scope=scope)

        method_char = "*" if method == "wildcard" else "~"

        query_string += "({scope}_city_name:{city_partial}{method_char})".format(scope=scope,
                                                                                 city_partial=search_text,
                                                                                 method_char=method_char)
        query = {
            "_source": ["{}_city_name".format(scope)],
            "size": 50000,
            "query": {
                "query_string": {
                    "query": query_string
                }
            },
        }
        if method == "fuzzy":
            query["query"]["query_string"]["fuzzy_prefix_length"] = 1

        response = OrderedDict(
            [("total-hits", 0), ("terms", [])]
        )

        hits = es_client_query(index=INDEX, body=query, retries=10)
        if hits:
            response["total-hits"] = hits["hits"]["total"]
            results = hits["hits"]["hits"]
            terms = []
            for result in results:
                if result["_source"]["{}_city_name".format(scope)]:
                    terms.append(result["_source"]["{}_city_name".format(scope)].strip())
            terms = set(terms)
            response["terms"] = terms
        return Response(response)
