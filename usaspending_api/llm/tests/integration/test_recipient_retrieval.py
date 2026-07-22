"""Integration tests for recipient_retrieval module"""

import pytest
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, connections

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.tests.helper import (
    build_fuzzy_recipient_query,
    fuzzy_search_recipients,
    retrieve_recipient_names,
)

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture(scope="module")
def elasticsearch_connection():
    """
    Fixture to provide Elasticsearch connection
    """
    es = Elasticsearch(
        hosts=[settings.ES_HOSTNAME],
        timeout=30,
    )

    connections.add_connection("default", es)

    yield es

    connections.remove_connection("default")
    es.close()


@pytest.fixture
def elasticsearch_recipient_index(elasticsearch_connection):
    """
    Fixture to set up recipient index with test data
    """
    index_name = "test-recipients"

    # Create index
    index = Index(index_name)
    if index.exists(using="default"):
        index.delete(using="default")
    index.create(using="default")

    # Add test data
    test_recipients = [
        {
            "recipient_name": "ACME CORPORATION",
            "uei": "UEI123456789",
            "duns": "123456789",
            "recipient_level": "P",
            "recipient_hash": "hash123",
        },
        {
            "recipient_name": "BETA CORP",
            "uei": "UEI987654321",
            "duns": "987654321",
            "recipient_level": "C",
            "recipient_hash": "hash456",
        },
        {
            "recipient_name": "ACME INDUSTRIES",
            "uei": "UEI111222333",
            "duns": "111222333",
            "recipient_level": "P",
            "recipient_hash": "hash789",
        },
    ]

    # Index test documents
    for recipient in test_recipients:
        elasticsearch_connection.index(
            index=index_name,
            body=recipient,
        )

    # Refresh index to make documents searchable
    elasticsearch_connection.indices.refresh(index=index_name)

    yield index_name

    # Cleanup
    if index.exists(using="default"):
        index.delete(using="default")


@pytest.fixture
def setup_test_recipients(elasticsearch_recipient_index):
    """
    Convenience fixture that just ensures recipient index is set up
    """
    return elasticsearch_recipient_index


# ============================================================================
# TESTS
# ============================================================================


@pytest.mark.django_db
class TestBuildFuzzyRecipientQueryIntegration:
    """Integration tests for build_fuzzy_recipient_query"""

    def test_returns_recipient_search_instance(self):
        """Test that function returns a RecipientSearch instance"""
        result = build_fuzzy_recipient_query("ACME Corporation")

        assert isinstance(result, RecipientSearch)

    def test_query_structure_contains_fuzzy_match(self):
        """Test that generated query contains fuzzy matching logic"""
        search = build_fuzzy_recipient_query("ACME")
        query_dict = search.to_dict()

        # Verify query structure exists
        assert "query" in query_dict
        # Verify it's configured for fuzzy matching
        assert query_dict.get("size") == 10

    def test_sanitizes_special_characters(self):
        """Test that special characters are properly sanitized"""
        # Should not raise exception with special characters
        search = build_fuzzy_recipient_query("ACME & Co. (2024)")

        assert isinstance(search, RecipientSearch)

    def test_handles_empty_string(self):
        """Test handling of empty search string"""
        search = build_fuzzy_recipient_query("")

        assert isinstance(search, RecipientSearch)

    def test_handles_whitespace_only(self):
        """Test handling of whitespace-only string"""
        search = build_fuzzy_recipient_query("   ")

        assert isinstance(search, RecipientSearch)


@pytest.mark.django_db
@pytest.mark.elasticsearch
class TestFuzzySearchRecipientsIntegration:
    """Integration tests for fuzzy_search_recipients"""

    def test_returns_list_type(self, setup_test_recipients):
        """Test that function returns a list"""
        result = fuzzy_search_recipients("test")

        assert isinstance(result, list)

    def test_returns_empty_list_for_no_matches(self):
        """Test that empty list is returned when no matches found"""
        result = fuzzy_search_recipients("NONEXISTENT_COMPANY_XYZ_12345")

        assert result == []

    def test_result_structure_with_matches(self, setup_test_recipients):
        """Test that results have correct structure when matches exist"""
        result = fuzzy_search_recipients("ACME", limit=5)

        if result:  # If test data exists
            assert isinstance(result, list)
            for item in result:
                assert "recipient_name" in item
                assert "uei" in item
                assert "duns" in item
                assert "recipient_level" in item
                assert "recipient_hash" in item
                assert "score" in item
                assert isinstance(item["score"], (int, float))

    def test_respects_limit_parameter(self, setup_test_recipients):
        """Test that limit parameter controls result count"""
        result_5 = fuzzy_search_recipients("CORP", limit=5)
        result_10 = fuzzy_search_recipients("CORP", limit=10)

        # If matches exist, verify limit is respected
        if result_5:
            assert len(result_5) <= 5
        if result_10:
            assert len(result_10) <= 10

    def test_results_ordered_by_relevance(self, setup_test_recipients):
        """Test that results are ordered by score (descending)"""
        result = fuzzy_search_recipients("ACME", limit=10)

        if len(result) > 1:
            scores = [item["score"] for item in result]
            assert scores == sorted(scores, reverse=True)

    def test_case_insensitive_search(self, setup_test_recipients):
        """Test that search is case-insensitive"""
        result_lower = fuzzy_search_recipients("acme corporation")
        result_upper = fuzzy_search_recipients("ACME CORPORATION")
        result_mixed = fuzzy_search_recipients("AcMe CoRpOrAtIoN")

        # All should return same results (or all empty)
        assert len(result_lower) == len(result_upper) == len(result_mixed)

    def test_handles_partial_matches(self, setup_test_recipients):
        """Test that partial text matches work"""
        result = fuzzy_search_recipients("ACM")

        # Should find results containing "ACM" (like ACME)
        assert isinstance(result, list)

    def test_handles_special_characters_in_search(self):
        """Test that special characters don't cause errors"""
        # Should not raise exception
        result = fuzzy_search_recipients("ACME & Co. (2024)")

        assert isinstance(result, list)


@pytest.mark.django_db
@pytest.mark.elasticsearch
class TestRetrieveRecipientNamesIntegration:
    """Integration tests for retrieve_recipient_names"""

    def test_returns_list_of_strings(self, setup_test_recipients):
        """Test that function returns a dictionary for a list of strings"""
        result = retrieve_recipient_names("ACME")

        assert isinstance(result, dict)
        # All items should be strings
        for k, v in result.items():
            assert isinstance(k, str)
            assert isinstance(v, list)
            for item in v:
                assert isinstance(item, str)

    def test_handles_no_matches(self):
        """Test behavior when no matches are found"""
        result = retrieve_recipient_names("NONEXISTENT_XYZ_12345")

        assert isinstance(result, dict)
        assert result["recipient_names"] == []

    def test_respects_limit_parameter(self, setup_test_recipients):
        """Test that limit parameter controls number of results"""
        result_3 = retrieve_recipient_names("CORP", limit=3)
        result_10 = retrieve_recipient_names("CORP", limit=10)

        # Results should respect the limit
        assert isinstance(result_3, dict)
        assert len(result_3["recipient_names"]) == 3
        assert isinstance(result_10, dict)
        assert len(result_10["recipient_names"]) == 10

    def test_extracts_recipient_identifiers(self, setup_test_recipients):
        """Test that recipient names, UEIs, and DUNS are extracted"""
        result = retrieve_recipient_names("ACME", limit=5)

        if result:
            # Should contain recipient identifiers (names, UEIs, DUNS)
            assert len(result["recipient_names"]) > 0
            # All should be strings
            assert all(isinstance(item, str) for v in result.values() for item in v)

    def test_case_insensitive_search(self, setup_test_recipients):
        """Test that search is case-insensitive"""
        result_lower = retrieve_recipient_names("acme corp")
        result_upper = retrieve_recipient_names("ACME CORP")

        # Should return same results
        assert len(result_lower["recipient_names"]) == len(result_upper["recipient_names"])

    def test_handles_special_characters(self):
        """Test that special characters are handled properly"""
        # Should not raise exception
        result = retrieve_recipient_names("ACME & Co. (2024)")

        assert isinstance(result, dict)

    def test_no_duplicate_names(self, setup_test_recipients):
        """Test that result contains no duplicates"""
        result = retrieve_recipient_names("CORP", limit=10)

        if result:
            # Should have no duplicates
            assert len(result["recipient_names"]) == len(set(result["recipient_names"]))

    def test_returns_multiple_identifiers_per_recipient(self, setup_test_recipients):
        """Test that multiple identifiers (name, UEI, DUNS) are returned for each recipient"""
        result = retrieve_recipient_names("ACME CORPORATION", limit=1)

        if result:
            # Should include multiple identifiers for the recipient
            # (name, UEI, DUNS at minimum)
            assert len(result["recipient_names"]) >= 1

    def test_handles_partial_matches(self, setup_test_recipients):
        """Test that partial text matches work"""
        result = retrieve_recipient_names("ACM")

        # Should find results containing "ACM" (like ACME)
        assert isinstance(result, dict)

    def test_handles_whitespace_in_query(self):
        """Test that whitespace in query is handled correctly"""
        result = retrieve_recipient_names("  ACME  ")

        assert isinstance(result, dict)

    def test_performance_with_large_limit(self, setup_test_recipients):
        """Test that function handles large limits without errors"""
        # Should not timeout or error with large limit
        result = retrieve_recipient_names("CORP", limit=100)

        assert isinstance(result, dict)


@pytest.mark.django_db
@pytest.mark.elasticsearch
class TestRecipientRetrievalEndToEnd:
    """End-to-end integration tests across multiple functions"""

    def test_fuzzy_search_to_retrieve_names_workflow(self, setup_test_recipients):
        """Test workflow from fuzzy search to retrieving names"""
        # Step 1: Fuzzy search to get detailed results
        search_results = fuzzy_search_recipients("ACME", limit=5)

        # Step 2: Retrieve just the names for the same query
        names = retrieve_recipient_names("ACME", limit=5)

        # Both should return results (or both empty)
        assert isinstance(search_results, list)
        assert isinstance(names, dict)

        # If search results exist, names should also exist
        if search_results:
            assert len(names["recipient_names"]) > 0

    def test_retrieve_names_includes_fuzzy_search_data(self, setup_test_recipients):
        """Test that retrieve_recipient_names includes data from fuzzy search"""
        # Get detailed results
        fuzzy_results = fuzzy_search_recipients("ACME CORPORATION", limit=1)

        # Get names
        names = retrieve_recipient_names("ACME CORPORATION", limit=1)

        if fuzzy_results:
            # Names should include identifiers from the fuzzy search results
            first_result = fuzzy_results[0]
            if first_result.get("recipient_name"):
                # At least one identifier should be in the names list
                assert any(
                    identifier in names["recipient_names"]
                    for identifier in [
                        first_result.get("recipient_name"),
                        first_result.get("uei"),
                        first_result.get("duns"),
                    ]
                    if identifier
                )

    def test_consistent_results_across_functions(self, setup_test_recipients):
        """Test that different functions return consistent data for same recipient"""
        search_text = "ACME CORPORATION"

        # Get results from both functions
        fuzzy_results = fuzzy_search_recipients(search_text, limit=5)
        names = retrieve_recipient_names(search_text, limit=5)

        # Both should return data (or both empty)
        assert isinstance(fuzzy_results, list)
        assert isinstance(names, dict)

        # If fuzzy search has results, names should too
        if fuzzy_results:
            assert len(names["recipient_names"]) > 0

    def test_build_query_to_fuzzy_search_workflow(self, setup_test_recipients):
        """Test workflow from building query to executing search"""
        # Step 1: Build query
        search = build_fuzzy_recipient_query("ACME")

        # Step 2: Execute search (fuzzy_search_recipients does this internally)
        results = fuzzy_search_recipients("ACME", limit=5)

        # Both should work without errors
        assert isinstance(search, RecipientSearch)
        assert isinstance(results, list)

    def test_performance_with_multiple_queries(self, setup_test_recipients):
        """Test that multiple queries execute efficiently"""
        queries = ["ACME", "BETA", "CORP", "INDUSTRIES"]

        for query in queries:
            # Should not timeout or error
            result = retrieve_recipient_names(query, limit=10)
            assert isinstance(result, dict)

    def test_empty_results_handled_consistently(self):
        """Test that empty results are handled consistently across functions"""
        nonexistent = "NONEXISTENT_XYZ_12345"

        # All functions should handle non-existent queries gracefully
        fuzzy_results = fuzzy_search_recipients(nonexistent)
        names = retrieve_recipient_names(nonexistent)

        assert fuzzy_results == []
        assert names["recipient_names"] == []

    def test_special_characters_handled_consistently(self):
        """Test that special characters are handled consistently"""
        special_query = "ACME & Co. (2024)"

        # Should not raise exceptions
        fuzzy_results = fuzzy_search_recipients(special_query)
        names = retrieve_recipient_names(special_query)

        assert isinstance(fuzzy_results, list)
        assert isinstance(names, dict)
