import logging
from typing import Any

from opensearchpy.helpers.query import Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.models.py_models import AITool, AIToolDescription
from usaspending_api.search.v2.es_sanitization import es_sanitize

logger = logging.getLogger(__name__)


# take a string name, uei or duns and get list of entities and subs
# uses recipient_retrieval.py


class RecipientLookupTool:
    """Tool for looking up recipients in OpenSearch with fuzzy matching support."""

    RECIPIENT_SOURCE_FIELDS = [
        "recipient_name",
        "uei",
        "duns",
        "recipient_level",
        "recipient_hash",
    ]

    def lookup_recipient(
        self,
        query: str,
        top_k: int = 10,
    ) -> list[str]:
        """
        Search for recipients by name, uei, duns, and return recipient names.
        """
        if not query or not query.strip():
            return []

        top_k = max(1, min(top_k, 100))
        query_upper = es_sanitize(query).strip().upper()

        try:
            search = self._build_search(query_upper, top_k)
            response = search.handle_execute()
        except Exception as exception:
            logger.error(f"OpenSearch query failed for query='{query}': {str(exception)}", exc_info=True)
            return []
        return self._extract_recipient_names(response)

    def _build_search(self, query_upper: str, top_k: int) -> RecipientSearch:
        should_queries = []
        for field in ("recipient_name", "uei", "duns"):
            should_queries.extend(
                [
                    ES_Q(
                        "term",
                        **{
                            f"{field}__keyword": {
                                "value": query_upper,
                                "boost": 10.0,
                            }
                        },
                    ),
                    ES_Q("match", **{field: {"query": query_upper, "boost": 8.0}}),
                    ES_Q("match", **{field: {"query": query_upper, "fuzziness": "AUTO", "boost": 5.0}}),
                    ES_Q("match", **{f"{field}__contains": {"query": query_upper, "boost": 3.0}}),
                    ES_Q("wildcard", **{f"{field}__keyword": {"value": f"{query_upper}*", "boost": 2.0}}),
                ]
            )
        should_queries_dict = [q.to_dict() for q in should_queries]

        return (
            RecipientSearch()
            .query("bool", should=should_queries_dict, minimum_should_match=1)
            .source(list(self.RECIPIENT_SOURCE_FIELDS))
            .sort({"_score": {"order": "desc"}})[:top_k]
        )

    def _extract_recipient_names(self, response: Any) -> list[str]:
        recipient_names = []
        seen_names = set()
        for hit in response.hits:
            recipient_name = hit.to_dict().get("recipient_name")
            if not recipient_name or recipient_name in seen_names:
                continue
            seen_names.add(recipient_name)
            recipient_names.append(recipient_name)
        return {"recipient_names": recipient_names}


lookup_recipient_tool = AITool(
    description=AIToolDescription(
        name="lookup_recipient",
        description="""
Search for valid recipient objects by name, UEI or DUNS using fuzzy matching.

Returns a list of strings.

Supported inputs:
- Recipient names (eg 'BOEING COMPANY', 'Lockheed Martin')
- UEI codes (12-character alphanumeric)
- DUNS numbers (9-digit, legacy)

Examples:
- lookup_recipient('BOEING') -> ['BOEING COMPANY', ...]
- lookup_recipient('EWN9HP5FT8A5') -> ['BOEING COMPANY', ...]

""".strip(),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Recipient search (name, uai, duns)"},
                "top_k": {
                    "type": "integer",
                    "description": "Maximum number of recipient results to return (1-100, default: 10)",
                },
            },
            "required": ["query"],
        },
    ),
    function=RecipientLookupTool().lookup_recipient,
    logging=lambda tool_input: f"Searching the recipient index for '{tool_input.get('query', 'N/A')}'",
)
