import logging
from typing import Any

from django.db.models import Q

from usaspending_api.llm.models.py_models import AITool, AIToolDescription
from usaspending_api.llm.tools.exceptions import AwardNotRecognizedError
from usaspending_api.search.models.award_search import AwardSearch

logger = logging.getLogger(__name__)

AWARD_ID_FIELDS = ("generated_unique_award_id", "generated_unique_award_id_legacy", "piid", "fain", "uri")


class AwardLookupTool:
    """Tool for confirming that an award ID actually exists."""

    async def lookup_award(self, query: str) -> dict[str, Any]:
        normalized = query.strip()
        if not normalized:
            return {"error": "Query cannot be empty", "results": {}}

        logger.info(f"Starting award lookup: query='{query}'", extra={"query": query})

        match = Q()
        for field in AWARD_ID_FIELDS:
            match |= Q(**{f"{field}__iexact": normalized})

        award = await AwardSearch.objects.filter(match).values(*AWARD_ID_FIELDS).afirst()

        if award is None:
            logger.warning(
                f"Zero results returned for award lookup: query='{query}'",
                extra={"query": query, "zero_results": True},
            )
            raise AwardNotRecognizedError(f"Unable to filter. We couldn't find an award with the ID: {query}")

        logger.info(f"Award lookup completed: query='{query}'", extra={"query": query})
        return {"result": award}


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

            Raises an error if no award matches the given ID.

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
