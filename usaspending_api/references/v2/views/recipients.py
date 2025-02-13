from collections import OrderedDict
from typing import Any, Dict, List, Union

from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
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
    def post(self, request: Request, format=None) -> Response:
        """
        Handle POST requests to the endpoint.
        Args:
            request: The request object containing data.
                data:{
                    "search_text": str
                    "recipient_levels": [""] (optional)
                    "limit": int (optional)
                    "duns": string (optional)
                }
            format: The format of the response (default=None).

        Returns:
            Returns a list of Recipients matching the input search_text and recipient_levels, if passed in.
        """
        search_text, recipient_levels = self._prepare_search_terms(request.data)
        limit = request.data["limit"]
        query = self._create_es_search(search_text, recipient_levels, limit)
        results = self._query_elasticsearch(query)
        response = OrderedDict([("count", len(results)), ("results", results), ("messages", [""])])
        return Response(response)

    def _prepare_search_terms(self, request_data: Dict[str, Union[str, List[str]]]) -> List[Union[str, List[str]]]:
        """
        Prepare search terms & recipient_levels from the request data.

        Args:
            request_data: The request data containing search text and optional recipient levels.

        Returns:
            A list containing the sanitized search text and recipient levels.
        """
        fields = [request_data["search_text"], request_data.get("recipient_levels", [])]
        return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]

    def _create_es_search(self, search_text: str, recipient_levels: List[str], limit: int) -> RecipientSearch:
        """
        Create an Elasticsearch search query for recipient autocomplete.

        Args:
            search_text: The search text entered by the user.
            recipient_levels: The list of recipient levels to filter by.
            duns: Any specific duns key specified by the user.
            limit: The maximum number of results to return.

        Returns:
            An Elasticsearch search query.
        """
        es_recipient_search_fields = ["recipient_name", "uei", "duns"]

        should_query = [
            query
            for search_field in es_recipient_search_fields
            for query in [
                ES_Q("match_phrase_prefix", **{f"{search_field}": {"query": search_text, "boost": 5}}),
                ES_Q("match_phrase_prefix", **{f"{search_field}.contains": {"query": search_text, "boost": 3}}),
                ES_Q("match", **{f"{search_field}": {"query": search_text, "operator": "and", "boost": 1}}),
            ]
        ]

        query = ES_Q("bool", should=should_query, minimum_should_match=1)

        if recipient_levels:
            recipient_should_clause = [
                ES_Q(
                    "bool",
                    should=[ES_Q("match", recipient_level=level) for level in recipient_levels],
                    minimum_should_match=1,
                )
            ]
            # if there are recipient levels, then any of the options from the recipient levels as well as the search text should match  # noqa: E501
            query = ES_Q("bool", must=[ES_Q("bool", should=recipient_should_clause), ES_Q("bool", should=query)])

        query = RecipientSearch().query(query)[:limit]
        return query

    def _query_elasticsearch(self, query: RecipientSearch) -> List[Dict[str, Any]]:
        """
        Query Elasticsearch with the given search query.

        Args:
            query: The Elasticsearch search query.

        Returns:
            A dictionary containing results from the Elasticsearch search query.
        """
        hits = query.handle_execute()
        results = []
        if (
            hits
            and "hits" in hits
            and "total" in hits["hits"]
            and "value" in hits["hits"]["total"]
            and hits["hits"]["total"]["value"] > 0
        ):  # noqa: E501
            results = self._parse_elasticsearch_response(hits)
        return results

    def _parse_elasticsearch_response(self, hits: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse Elasticsearch response and extract relevant information.

        Args:
            hits: The Elasticsearch response containing search hits.

        Returns:
            A dictionary containing parsed search results (recipient_name, uei, and recipient_level).
        """
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
                        ("duns", recipient["duns"] if "duns" in recipient else None),
                    ]
                )
            )
        return results
