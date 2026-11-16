"""
How many awards a saved Smart Assist filter set returns, by award type.

This reuses the code behind the website's results-page tab counts (the spending_by_award_count endpoint), so the numbers
here match what the user sees once the filters are applied. The check is advisory: any failure returns None so that it
can never fail a search.
"""

import asyncio
import logging

from asgiref.sync import sync_to_async

from usaspending_api.llm.tools.helpers.search_filters import to_search_filters
from usaspending_api.search.v2.views.spending_by_award_count import SpendingByAwardCountVisualizationViewSet

logger = logging.getLogger(__name__)

RESULT_COUNT_TIMEOUT_SECONDS = 5


async def get_award_counts(filter_request: dict) -> dict[str, int] | None:
    """Award counts per award type for a saved filter request, or None if they could not be determined."""
    try:
        search_filters = to_search_filters(filter_request)
        view = SpendingByAwardCountVisualizationViewSet()
        return await asyncio.wait_for(
            sync_to_async(view.query_elasticsearch_for_prime_awards)(search_filters),
            timeout=RESULT_COUNT_TIMEOUT_SECONDS,
        )
    except Exception as e:
        logger.warning(f"Could not get award counts for the selected filters: {e!r}", exc_info=True)
        return None


def total_award_count(counts: dict[str, int]) -> int:
    return sum(counts.values())
