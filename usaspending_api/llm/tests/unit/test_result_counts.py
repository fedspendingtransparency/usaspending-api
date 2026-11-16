import time
from unittest.mock import patch

import pytest

from usaspending_api.llm.tools.helpers import result_counts
from usaspending_api.llm.tools.helpers.result_counts import get_award_counts, total_award_count

VIEW_METHOD = (
    "usaspending_api.llm.tools.helpers.result_counts.SpendingByAwardCountVisualizationViewSet"
    ".query_elasticsearch_for_prime_awards"
)
COUNTS = {"contracts": 3, "idvs": 0, "grants": 2, "direct_payments": 0, "loans": 0, "other": 1}


@pytest.mark.asyncio
async def test_returns_the_counts_from_the_search_endpoint_code():
    with patch(VIEW_METHOD, return_value=COUNTS) as query:
        result = await get_award_counts({"filters": {"keyword": {"housing": "housing"}, "awardType": ["A"]}})

    assert result == COUNTS
    # the saved (website-shaped) filters are converted to the search API shape first
    query.assert_called_once_with({"keywords": ["housing"], "award_type_codes": ["A"]})


@pytest.mark.asyncio
async def test_returns_none_when_the_query_fails():
    with patch(VIEW_METHOD, side_effect=RuntimeError("opensearch is down")):
        assert await get_award_counts({"filters": {"keyword": ["housing"]}}) is None


@pytest.mark.asyncio
async def test_returns_none_when_the_filters_cannot_be_converted():
    with patch(VIEW_METHOD) as query:
        result = await get_award_counts({"filters": {"selectedAwardingAgencies": {"1": {"agencyType": "toptier"}}}})

    assert result is None
    query.assert_not_called()


@pytest.mark.asyncio
async def test_returns_none_when_the_query_is_too_slow(monkeypatch):
    monkeypatch.setattr(result_counts, "RESULT_COUNT_TIMEOUT_SECONDS", 0.05)
    with patch(VIEW_METHOD, side_effect=lambda _: time.sleep(0.3)):
        assert await get_award_counts({"filters": {"keyword": ["housing"]}}) is None


def test_total_award_count():
    assert total_award_count(COUNTS) == 6
    assert total_award_count({"contracts": 0, "grants": 0}) == 0
