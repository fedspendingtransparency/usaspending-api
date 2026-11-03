import logging
from typing import Any

from opensearchpy.helpers.query import Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import AsyncAwardSearch
from usaspending_api.llm.models.py_models import AITool, AIToolDescription, AIToolResult
from usaspending_api.llm.tools.exceptions import AwardNotRecognizedError

logger = logging.getLogger(__name__)

AWARD_SOURCE_FIELDS = ("generated_unique_award_id", "display_award_id", "piid", "fain", "uri")

# Term lookups on keyword fields return in milliseconds, so anything near this limit means the cluster is
# unhealthy. Failing here keeps a stuck query from holding the request open until the gateway returns a 504.
ES_LOOKUP_TIMEOUT_SECONDS = 10


class AwardLookupTool:
    """Tool for confirming that an award ID actually exists."""

    NO_MATCH_INSTRUCTIONS = (
        "STOP AND READ: no award matched this ID. Do NOT tell the user there is no data for it yet. "
        "Identifiers that look like award IDs are often RECIPIENT identifiers instead -- a UEI "
        "(12-character alphanumeric, e.g. HR1JA12FSM63), a DUNS number (9 digits), or a recipient name "
        "or abbreviation. You MUST call lookup_recipient with this value before concluding there is no "
        "data or ending the conversation. If lookup_recipient finds a match, filter with "
        "selectedRecipients instead of selectedAwardIDs."
    )

    async def lookup_award(self, query: str) -> AIToolResult:
        normalized = query.strip()
        if not normalized:
            return AIToolResult(results={"error": "Query cannot be empty", "results": {}})

        logger.info(f"Starting award lookup: query='{query}'", extra={"query": query})

        award = await self._find_award(normalized.upper(), query)

        if award is None:
            logger.warning(
                f"Zero results returned for award lookup: query='{query}'",
                extra={"query": query, "zero_results": True},
            )
            user_message = f"Unable to filter. We couldn't find an award with the ID: {query}"
            raise AwardNotRecognizedError(user_message, model_detail=f"{user_message}. {self.NO_MATCH_INSTRUCTIONS}")

        award_id = display_award_id(award)
        logger.info(f"Award lookup completed: query='{query}'", extra={"query": query})
        return AIToolResult(
            results={
                "award_id": award_id,
                "award": award,
            },
            instructions=f"Use exactly '{award_id}' in selectedAwardIDs. Do NOT use generated_unique_award_id "
            "(CONT_AWD_.../ASST_...) values there -- the award ID filter only matches PIID/FAIN/URI.",
        )

    async def _find_award(self, query_upper: str, query: str) -> dict[str, Any] | None:
        search = self._build_search(query_upper)
        try:
            response = await search.handle_execute(retries=1, timeout=f"{ES_LOOKUP_TIMEOUT_SECONDS}s")
        finally:
            if search.client is not None:
                await search.client.close()

        # A search-side timeout returns partial (often empty) hits, which would read as "award not found".
        if response.timed_out:
            raise TimeoutError(f"OpenSearch award lookup timed out: query='{query}'")

        return response.hits[0].to_dict() if response.hits else None

    @staticmethod
    def _build_search(query_upper: str) -> AsyncAwardSearch:
        # A generated unique award ID is unique, while a PIID/FAIN/URI can be shared by several awards, so
        # boost the unique match to make it the top hit if both kinds match.
        should_queries = [
            ES_Q("term", generated_unique_award_id={"value": query_upper, "boost": 10.0}).to_dict(),
            ES_Q("term", display_award_id={"value": query_upper, "boost": 2.0}).to_dict(),
            *(ES_Q("term", **{f"{field}__keyword": query_upper}).to_dict() for field in ("piid", "fain", "uri")),
        ]
        return (
            AsyncAwardSearch()
            .params(request_timeout=ES_LOOKUP_TIMEOUT_SECONDS)
            .query("bool", should=should_queries, minimum_should_match=1)
            .source(list(AWARD_SOURCE_FIELDS))[:1]
        )


def display_award_id(award: dict[str, Any]) -> str:
    """The ID the advanced search award ID filter matches on (PIID, FAIN, or URI)."""
    return award.get("display_award_id") or award.get("piid") or award.get("fain") or award.get("uri")


lookup_award_tool = AITool(
    function=AwardLookupTool().lookup_award,
    logging=lambda tool_input: f"Looking up award '{tool_input.get('query', 'N/A')}'",
    description=AIToolDescription(
        name="lookup_award",
        description="""
            Confirm that an award ID actually exists before using it in selectedAwardIDs.

            Matches exactly (case-insensitive) against an award's generated unique award ID, PIID,
            FAIN, or URI. This is an exact-identifier lookup, not a
            fuzzy search - use it to verify an award ID you already have, not to search for
            awards by description or keyword.

            On a match, returns "award_id" -- the award's PIID, FAIN, or URI. Always put that
            "award_id" value in selectedAwardIDs, never generated_unique_award_id: the award ID
            filter only matches PIID/FAIN/URI.

            Raises an error if no award matches the given ID. When that happens, the value may be a
            recipient identifier (UEI, DUNS) or name rather than an award ID -- you MUST call
            lookup_recipient with it before concluding there's no data.

            Examples:
            - lookup_award('CONT_AWD_W91278_9700_...') -> Exact match on generated_unique_award_id.
            - lookup_award('W91CRB18D0001') -> Exact match on piid.
        """.strip(),
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Award ID to confirm (generated unique award ID, PIID, FAIN, or URI)",
                },
            },
            "required": ["query"],
        },
    ),
)
