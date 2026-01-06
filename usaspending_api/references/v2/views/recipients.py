import itertools
from collections import OrderedDict
from typing import Any, Union

from elasticsearch_dsl import A, Q as ES_Q
from elasticsearch_dsl.response import AggResponse
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

    limit: int
    recipient_levels: list[str] | None
    search_text: str

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/recipient.md"
    search_fields = ["recipient_name", "uei", "duns"]

    @cache_response()
    def post(self, request: Request) -> Response:
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

        Returns:
            Returns a list of Recipients matching the input search_text and recipient_levels, if passed in.
        """
        self.search_text, self.recipient_levels = self._prepare_search_terms(request.data)
        self.limit = request.data["limit"]
        search = self._create_es_search()
        results = self._query_elasticsearch(search)
        recipient_level_response_message = (
            "The response value of 'recipient_level' is deprecated to avoid to confusion when returning"
            " only a single Recipient in the response. The request value of 'recipient_levels' will"
            " continue to limit the response."
        )
        response = OrderedDict(
            [("count", len(results)), ("results", results), ("messages", [recipient_level_response_message])]
        )
        return Response(response)

    @staticmethod
    def _prepare_search_terms(request_data: dict[str, Union[str, list[str]]]) -> list[Union[str, list[str]]]:
        """
        Prepare search terms & recipient_levels from the request data.

        Args:
            request_data: The request data containing search text and optional recipient levels.

        Returns:
            A list containing the sanitized search text and recipient levels.
        """
        fields = [request_data["search_text"], request_data.get("recipient_levels", [])]
        return [es_sanitize(field).upper() if isinstance(field, str) else field for field in fields]

    def _create_es_search(self) -> RecipientSearch:
        """
        Create an Elasticsearch search query for recipient autocomplete.

        Returns:
            An Elasticsearch search query.
        """
        should_queries = {
            search_field: [
                ES_Q("match_phrase_prefix", **{f"{search_field}": {"query": self.search_text, "boost": 5}}),
                ES_Q("match_phrase_prefix", **{f"{search_field}.contains": {"query": self.search_text, "boost": 3}}),
                ES_Q("match", **{f"{search_field}": {"query": self.search_text, "operator": "and", "boost": 1}}),
            ]
            for search_field in self.search_fields
        }
        query = ES_Q(
            "bool", should=list(itertools.chain.from_iterable(should_queries.values())), minimum_should_match=1
        )

        if self.recipient_levels:
            recipient_should_clause = [
                ES_Q(
                    "bool",
                    should=[ES_Q("match", recipient_level=level) for level in self.recipient_levels],
                    minimum_should_match=1,
                )
            ]
            # if there are recipient levels, then any of the options from the recipient levels as well
            # as the search text should match
            query = ES_Q("bool", must=[ES_Q("bool", should=recipient_should_clause), ES_Q("bool", should=query)])

        search = RecipientSearch().query(query)

        # Size is set to 1 as way to easily check if any results were found
        search.update_from_dict({"size": 1})

        for search_field, query in should_queries.items():
            search.aggs.bucket(
                f"filter_{search_field}",
                self._build_filter_aggregation(search_field, query, search_field != "recipient_name"),
            )

        return search

    def _query_elasticsearch(self, search: RecipientSearch) -> list[dict[str, Any]]:
        """
        Query Elasticsearch with the given search query.

        Args:
            search: An Elasticsearch RecipientSearch object.

        Returns:
            A dictionary containing results from the Elasticsearch search query.
        """
        response = search.handle_execute()
        results = self._parse_elasticsearch_response(response.aggregations) if response.hits else []
        return results

    def _parse_elasticsearch_response(self, response_aggregations: AggResponse) -> list[dict[str, Any]]:
        """
        Parse Elasticsearch response and extract relevant information.

        Args:
            response: The Elasticsearch response containing search hits.

        Returns:
            A dictionary containing parsed search results (recipient_name, uei, and recipient_level).
        """
        results = []
        result_template = dict.fromkeys(["recipient_name", "recipient_level", "uei", "duns"])
        for search_field in self.search_fields:
            filtered_subset = getattr(response_aggregations, f"filter_{search_field}")
            for bucket in getattr(filtered_subset, f"unique_{search_field}"):
                temp_result = {search_field: bucket.key}
                if search_field != "recipient_name":
                    temp_result["recipient_name"] = bucket.recipient_details[0].recipient_name
                results.append(
                    {
                        **result_template,
                        **temp_result,
                    }
                )

        return results

    def _build_filter_aggregation(self, field_name: str, query: ES_Q, include_recipient_name: bool) -> A:
        sub_query_filter = A(f"filter", ES_Q("bool", should=query, minimum_should_match=1))
        unique_field_agg = A("terms", field=f"{field_name}.keyword", size=self.limit)

        if include_recipient_name:
            top_hits_agg = A("top_hits", size=1, _source={"includes": "recipient_name"})
            unique_field_agg.bucket("recipient_details", top_hits_agg)

        sub_query_filter.bucket(f"unique_{field_name}", unique_field_agg)

        return sub_query_filter
