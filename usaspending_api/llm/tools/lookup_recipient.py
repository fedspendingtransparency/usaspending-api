import logging
from typing import Any

from opensearchpy.helpers.query import Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.models.py_models import AITool, AIToolDescription
from usaspending_api.search.v2.es_sanitization import es_sanitize

logger = logging.getLogger(__name__)


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

        logger.info(
            f"Starting recipient lookup: query='{query}', top_k={top_k}",
            extra={"query": query, "top_k": top_k},
        )

        try:
            search = self._build_search(query_upper, top_k)
            logger.debug(f"Executing OpenSearch query for recipient: query='{query}'")
            response = search.handle_execute()
            logger.info(
                f"OpenSearch query successful: query='{query}', hits={len(response.hits)}",
                extra={"query": query, "hits_count": len(response.hits)},
            )
        except Exception as exception:
            logger.error(f"OpenSearch query failed for query='{query}': {str(exception)}", exc_info=True)
            return []

        result = self._extract_recipient_names(response, query_upper)
        recipient_count = len(result.get("recipient_names", []))

        # Log zero results as a warning for quality monitoring.
        if recipient_count == 0:
            logger.warning(
                f"Zero results returned for recipient lookup: query='{query}'",
                extra={
                    "query": query,
                    "hits_count": len(response.hits),
                    "zero_results": True,
                },
            )

        logger.info(
            f"Recipient lookup completed: query='{query}', recipient_names_count={recipient_count}",
            extra={"query": query, "recipient_names_count": recipient_count},
        )
        return result

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
                    ES_Q("match_phrase_prefix", **{f"{field}__contains": {"query": query_upper, "boost": 3.0}}),
                    ES_Q("wildcard", **{f"{field}__keyword": {"value": f"{query_upper}*", "boost": 2.0}}),
                ]
            )

        should_queries.append(ES_Q("term", **{"recipient_hash": {"value": query_upper.lower(), "boost": 10.0}}))
        should_queries_dict = [q.to_dict() for q in should_queries]

        return (
            RecipientSearch()
            .query("bool", should=should_queries_dict, minimum_should_match=1)
            .source(list(self.RECIPIENT_SOURCE_FIELDS))
            .sort({"_score": {"order": "desc"}})[:top_k]
        )

    def _extract_recipient_names(self, response: Any, query_upper: str) -> list[str]:
        recipient_names = []
        seen_values = set()
        for hit in response.hits:
            hit_dict = hit.to_dict()
            # When the query exactly matches an identifier (UEI, DUNS, or hash), that is an
            # unambiguous hit: return only the matched identifier the caller searched by.
            matched_identifier = self._matched_identifier(hit_dict, query_upper)
            if matched_identifier:
                return {"recipient_names": [matched_identifier]}

            recipient_name = hit_dict.get("recipient_name")
            # An exact name match is also unambiguous: return only that name.
            if recipient_name and str(recipient_name).upper() == query_upper:
                return {"recipient_names": [recipient_name]}

            if not recipient_name or recipient_name in seen_values:
                continue
            seen_values.add(recipient_name)
            recipient_names.append(recipient_name)
        return {"recipient_names": recipient_names}

    @staticmethod
    def _matched_identifier(hit_dict: dict, query_upper: str) -> str | None:
        """Return the UEI, DUNS, or hash if the query is an exact match for it, else None."""
        for field in ("uei", "duns", "recipient_hash"):
            identifier = hit_dict.get(field)
            if identifier and str(identifier).upper() == query_upper:
                return identifier
        return None


lookup_recipient_tool = AITool(
    description=AIToolDescription(
        name="lookup_recipient",
        description="""
Search for valid recipient objects by name, UEI, DUNS, or recipient hash using fuzzy matching.

Returns a list of strings. When the query is an exact match for a recipient (by UEI, DUNS,
recipient hash, or name), a single-element list containing just that exact match is returned
(the matching identifier for identifier matches, otherwise the name). When there is no exact
match, the list of fuzzy-matched recipient names is returned.

Supported inputs:
- Recipient names (eg 'BOEING COMPANY', 'Lockheed Martin')
- UEI codes (12-character alphanumeric)
- DUNS numbers (9-digit, legacy)
- Recipient hashes (UUID)

Examples:
- lookup_recipient('BOEING') -> ['BOEING COMPANY', ...]
- lookup_recipient('EWN9HP5FT8A5') -> ['EWN9HP5FT8A5', ...]

""".strip(),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Recipient search (name, uei, duns, hash)"},
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
