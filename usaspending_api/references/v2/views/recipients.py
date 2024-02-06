from collections import OrderedDict

from elasticsearch_dsl import Q as ES_Q
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.common.validator.tinyshield import validate_post_request
from usaspending_api.search.v2.es_sanitization import es_sanitize

models = [
    {"name": "search_text", "key": "search_text", "type": "text", "text_type": "search", "optional": False},
    {"name": "limit", "key": "limit", "type": "integer", "max": 500, "optional": True, "default": 10},
    {
        "name": "recipient_levels",
        "key": "recipient_levels",
        "type": "array",
        "array_type": "text",
        "text_type": "search",
        "items": {"type": "string"},
        "optional": True,
    },
]


@validate_post_request(models)
class RecipientAutocompleteViewSet(APIView):
    """
    This endpoint is used for the Recipient autocomplete filter which returns a list of recipients matching the
    specified search text.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/recipient.md"

    @cache_response()
    def post(self, request, format=None):
        search_text, recipient_levels = prepare_search_terms(request.data)
        limit = request.data["limit"]
        query = create_es_search(search_text, recipient_levels, limit)
        results = query_elasticsearch(query)
        response = OrderedDict([("count", len(results)), ("results", results)])
        return Response(response)


def prepare_search_terms(request_data):
    fields = [request_data["search_text"], request_data.get("recipient_levels", [])]
    return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]


def create_es_search(search_text, recipient_levels, limit):
    ES_RECIPIENT_SEARCH_FIELDS = ["recipient_name", "uei"]

    query = ES_Q()
    # Build the should_clauses list
    search_text_should_clause = [
        ES_Q(
            "bool",
            should=[
                ES_Q("query_string", query=search_text, fields=ES_RECIPIENT_SEARCH_FIELDS),
                ES_Q("match", recipient_name=search_text),
                ES_Q("match", uei=search_text),
            ],
            minimum_should_match=1,
        )
    ]

    if recipient_levels:
        recipient_should_clause = [
            ES_Q(
                "bool",
                should=[ES_Q("match", recipient_level=level) for level in recipient_levels],
                minimum_should_match=1,
            )
        ]

        query = ES_Q(
            "bool", must=[ES_Q("bool", should=recipient_should_clause), ES_Q("bool", should=search_text_should_clause)]
        )

    else:
        query = search_text_should_clause

    query = RecipientSearch().query(query)[:limit]
    return query


def query_elasticsearch(query):
    hits = query.handle_execute()
    results = []
    if hits and hits["hits"]["total"]["value"] > 0:
        results = parse_elasticsearch_response(hits)
    return results


def parse_elasticsearch_response(hits):
    recipients = hits["hits"]["hits"]
    results = []
    for temp in recipients:
        recipient = temp["_source"]
        results.append(
            OrderedDict(
                [
                    ("recipient_name", recipient["recipient_name"]),
                    ("uei", recipient["uei"]),
                    ("recipient_level", recipient["recipient_level"]),
                ]
            )
        )
    return results
