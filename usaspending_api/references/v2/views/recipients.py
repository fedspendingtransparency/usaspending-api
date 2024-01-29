from rest_framework.response import Response
from rest_framework.views import APIView
from collections import OrderedDict
from elasticsearch_dsl import Q as ES_Q, A

from usaspending_api.common.cache_decorator import cache_response

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.search.v2.es_sanitization import es_sanitize
from usaspending_api.common.validator.tinyshield import validate_post_request

models = [
    {"key": "search_text", "name": "search_text", "type": "text", "text_type": "search", "optional": False},
    {"key": "limit", "name": "limit", "type": "integer", "max": 500, "optional": True, "default": 10},
    {"key": "recipient_levels", "name": "recipient_levels", "type": "array", "items": {"type": "string"}, "optional": True},
]

@validate_post_request(models)
class RecipientAutocompleteViewSet(APIView):
    """
    This endpoint is used for the Recipient autocomplete filter which returns a list of recipients matching the specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/recipient.md"

    @cache_response()
    def post(self, request, format=None):
        search_text, recipient_levels = prepare_search_terms(request.data)
        limit = request.data["limit"]

        query = create_elasticsearch_query(search_text, recipient_levels, limit)
        sorted_results = query_elasticsearch(query)
        response = OrderedDict([("count", len(sorted_results)), ("results", sorted_results[:limit])])

        return Response(response)


def prepare_search_terms(request_data):
    fields = [request_data["search_text"], request_data.get("recipient_levels", [])]
    return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]

def create_elasticsearch_query(search_text, recipient_levels, limit):
    query = create_es_search(search_text, recipient_levels)
    query.aggs.bucket("recipients", A("terms", field="recipient_name.keyword", size=limit))
    return query

 # search_text = "kimberly"  # Test recipient name. There is a recipient with this name and a level of "R"
def create_es_search(search_text, recipient_levels):
    ES_RECIPIENT_SEARCH_FIELDS = ["recipient_name", "uei"]

    if recipient_levels:
        # Only return the matches where the `recipient_level` field contains the filter value from
        #   the HTTP request.
        query = ES_Q(
            "bool",
            must=[
                ES_Q("match", recipient_level="C"),
                ES_Q("query_string", query=f"*{search_text}*", fields=ES_RECIPIENT_SEARCH_FIELDS)
            ]
        )
    else:
        # If we're NOT filtering on the `recipient_level` field then return all results matching the given search text
        query = ES_Q("query_string", query=f"*{search_text}*", fields=ES_RECIPIENT_SEARCH_FIELDS)

    search = RecipientSearch().filter(query)
    return search

def query_elasticsearch(query):
    hits = query.handle_execute()
    results = []
    if hits and hits["hits"]["total"]["value"] > 0:
        results = parse_elasticsearch_response(hits)
        results = sorted(results, key=lambda x: x.pop("hits"), reverse=True)
    return results

def parse_elasticsearch_response(hits):
    results = []
    for recipient in hits["aggregations"]["recipients"]["buckets"]:
        results.append(
            OrderedDict(
                [
                    ("recipient_name", recipient["key"]),
                    ("hits", recipient["doc_count"]),
                ]
            )
        )
    return results
