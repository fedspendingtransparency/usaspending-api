"""
Unit tests for spending_by_award pagination bug fix (Issue #4793)
Tests that hasNext correctly reflects availability of more pages beyond 10,000 records
"""

from unittest.mock import Mock

from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.search.v2.views.spending_by_award import SpendingByAwardVisualizationViewSet


def create_mock_es_response(num_results, total_value=10000):
    """Create a mock Elasticsearch response with specified number of results"""
    mock_response = Mock()
    mock_response.hits.total.value = total_value

    # Create mock results with meta.sort attributes
    mock_results = []
    for i in range(num_results):
        mock_result = Mock()
        mock_result.meta.sort = [f"sort_value_{i}", f"unique_id_{i}"]
        mock_results.append(mock_result)

    # Make the response iterable and support len()
    mock_response.__iter__ = lambda self: iter(mock_results)
    mock_response.__len__ = lambda self: len(mock_results)
    mock_response.__getitem__ = lambda self, idx: mock_results[idx]

    return mock_response


class TestSpendingByAwardPaginationBeyond10k:
    """Tests for pagination behavior at the 10,000 record boundary"""

    def test_has_next_true_at_page_99_with_limit_100(self):
        """Page 99 (records 9800-9899) should have hasNext=True when more records exist"""
        view = SpendingByAwardVisualizationViewSet()
        view.pagination = {"page": 99, "limit": 100}
        view.last_record_unique_id = None
        view.last_record_sort_value = None
        view.spending_level = SpendingLevel.AWARD
        view.original_filters = {}

        # Mock response with 101 results (limit + 1), indicating more pages exist
        # The total.value is capped at 10,000 (Elasticsearch default)
        mock_response = create_mock_es_response(num_results=101, total_value=10000)
        mock_results = list(mock_response)

        result = view.construct_es_response(mock_results, mock_response)

        assert result["page_metadata"]["hasNext"] is True, (
            "hasNext should be True when we fetch 101 results (indicating page 100 exists)"
        )
        assert len(result["results"]) == 100, "Should return exactly limit results, not the peek record"
        assert result["page_metadata"]["last_record_unique_id"] == "unique_id_99", (
            "Cursor should point to last returned record (index 99), not the peek record"
        )

    def test_has_next_true_at_page_100_with_limit_100(self):
        """Page 100 (records 9900-9999) should have hasNext=True when page 101 exists

        This is the key bug fix - previously hasNext would be False at exactly 10,000 records
        because response.hits.total.value caps at 10,000
        """
        view = SpendingByAwardVisualizationViewSet()
        view.pagination = {"page": 100, "limit": 100}
        view.last_record_unique_id = None
        view.last_record_sort_value = None
        view.spending_level = SpendingLevel.AWARD
        view.original_filters = {}

        # Mock response with 101 results (limit + 1), indicating more pages exist
        # The OLD buggy calculation would use response.hits.total.value = 10000:
        #   10000 - (100-1)*100 = 100, which is NOT > 100 → hasNext=False ❌
        # The NEW correct calculation uses len(results) > limit:
        #   101 > 100 → hasNext=True ✅
        mock_response = create_mock_es_response(num_results=101, total_value=10000)
        mock_results = list(mock_response)

        result = view.construct_es_response(mock_results, mock_response)

        assert result["page_metadata"]["hasNext"] is True, (
            "hasNext should be True at page 100 when 101 results returned (page 101 exists)"
        )

    def test_has_next_false_at_last_page(self):
        """Last page should have hasNext=False when exactly limit or fewer results returned"""
        view = SpendingByAwardVisualizationViewSet()
        view.pagination = {"page": 150, "limit": 100}
        view.last_record_unique_id = None
        view.last_record_sort_value = None
        view.spending_level = SpendingLevel.AWARD
        view.original_filters = {}

        # Mock response with exactly 100 results (no extra), indicating this is the last page
        mock_response = create_mock_es_response(num_results=100, total_value=10000)
        mock_results = list(mock_response)

        result = view.construct_es_response(mock_results, mock_response)

        assert result["page_metadata"]["hasNext"] is False, (
            "hasNext should be False when we fetch exactly limit results (no more pages)"
        )

    def test_has_next_with_different_page_sizes_at_10k_boundary(self):
        """Test various page sizes at the 10,000 boundary to ensure consistent behavior"""
        test_cases = [
            # (page, limit, num_results, expected_hasNext)
            (200, 50, 51, True),  # Page 200 with limit 50: records 9950-9999, has page 201
            (200, 50, 50, False),  # Page 200 with limit 50: last page
            (400, 25, 26, True),  # Page 400 with limit 25: records 9975-9999, has page 401
            (400, 25, 25, False),  # Page 400 with limit 25: last page
        ]

        for page, limit, num_results, expected_has_next in test_cases:
            view = SpendingByAwardVisualizationViewSet()
            view.pagination = {"page": page, "limit": limit}
            view.last_record_unique_id = None
            view.last_record_sort_value = None
            view.spending_level = SpendingLevel.AWARD
            view.original_filters = {}

            mock_response = create_mock_es_response(num_results=num_results, total_value=10000)
            mock_results = list(mock_response)

            result = view.construct_es_response(mock_results, mock_response)

            assert result["page_metadata"]["hasNext"] is expected_has_next, (
                f"Page {page} with limit {limit} and {num_results} results: expected hasNext={expected_has_next}"
            )

    def test_search_after_pagination_unchanged(self):
        """Verify search_after pagination (with last_record_unique_id) still works correctly"""
        view = SpendingByAwardVisualizationViewSet()
        view.pagination = {"page": 1, "limit": 100}
        view.last_record_unique_id = "some_unique_id"
        view.last_record_sort_value = "some_sort_value"
        view.spending_level = SpendingLevel.AWARD
        view.original_filters = {}

        # With search_after, hasNext is calculated by len(results) > limit
        # This should be unchanged by our fix
        mock_response = create_mock_es_response(num_results=101, total_value=10000)
        mock_results = list(mock_response)

        result = view.construct_es_response(mock_results, mock_response)

        assert result["page_metadata"]["hasNext"] is True, (
            "search_after pagination should still use len(results) > limit"
        )

        # Test when no more results
        mock_response_last = create_mock_es_response(num_results=50, total_value=10000)
        mock_results_last = list(mock_response_last)

        result_last = view.construct_es_response(mock_results_last, mock_response_last)

        assert result_last["page_metadata"]["hasNext"] is False, (
            "search_after pagination should return hasNext=False when fewer than limit results"
        )
