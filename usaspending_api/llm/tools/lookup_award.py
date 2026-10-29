import logging
from typing import Any

from django.db.models import Q

from usaspending_api.llm.models.py_models import AITool, AIToolDescription, AIToolResult
from usaspending_api.llm.tools.exceptions import AwardNotRecognizedError
from usaspending_api.search.models.award_search import AwardSearch

logger = logging.getLogger(__name__)

AWARD_ID_FIELDS = ("generated_unique_award_id", "generated_unique_award_id_legacy", "piid", "fain", "uri")


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

        match = Q()
        for field in AWARD_ID_FIELDS:
            match |= Q(**{f"{field}__iexact": normalized})

        award = await AwardSearch.objects.filter(match).values(*AWARD_ID_FIELDS, "display_award_id").afirst()

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

            Matches exactly (case-insensitive) against an award's generated unique award ID
            (current or legacy), PIID, FAIN, or URI. This is an exact-identifier lookup, not a
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
