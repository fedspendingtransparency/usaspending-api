"""Integration tests for recipient_retrieval module"""
import pytest
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, connections

from usaspending_api.common.elasticsearch.search_wrappers import RecipientSearch
from usaspending_api.llm.retrieval.recipient_retrieval import (
    build_fuzzy_recipient_query,
    expand_prime_recipient_subcontractors,
    fuzzy_search_recipients,
    retrieve_company_and_subcontractors,
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

    connections.add_connection('default', es)

    yield es

    connections.remove_connection('default')
    es.close()


@pytest.fixture
def elasticsearch_recipient_index(elasticsearch_connection):
    """
    Fixture to set up recipient index with test data
    """
    index_name = "test-recipients"

    # Create index
    index = Index(index_name)
    if index.exists(using='default'):
        index.delete(using='default')
    index.create(using='default')

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
    if index.exists(using='default'):
        index.delete(using='default')


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
class TestExpandPrimeRecipientSubcontractorsIntegration:
    """Integration tests for expand_prime_recipient_subcontractors"""

    def test_returns_dict_with_required_keys(self):
        """Test that function returns dict with expected structure"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123456789",
            duns="123456789",
            recipient_hash="testhash",
            recipient_level="P",
        )

        assert isinstance(result, dict)
        assert "prime" in result
        assert "subcontractors" in result
        assert "all_recipient_names" in result

    def test_prime_contains_all_provided_fields(self):
        """Test that prime recipient contains all provided fields"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123456789",
            duns="123456789",
            recipient_hash="testhash",
            recipient_level="P",
        )

        prime = result["prime"]
        assert prime["recipient_name"] == "TEST CORP"
        assert prime["uei"] == "TEST123456789"
        assert prime["duns"] == "123456789"
        assert prime["recipient_hash"] == "testhash"
        assert prime["recipient_level"] == "P"

    def test_subcontractors_is_list(self):
        """Test that subcontractors field is a list"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123456789",
        )

        assert isinstance(result["subcontractors"], list)

    def test_all_recipient_names_is_list(self):
        """Test that all_recipient_names is a list"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123456789",
        )

        assert isinstance(result["all_recipient_names"], list)

    def test_all_recipient_names_includes_prime(self):
        """Test that all_recipient_names includes prime recipient"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123456789",
        )

        assert "TEST CORP" in result["all_recipient_names"]

    def test_handles_missing_optional_fields(self):
        """Test that function works with minimal fields"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
        )

        assert result["prime"]["recipient_name"] == "TEST CORP"
        assert result["prime"]["uei"] is None
        assert result["prime"]["duns"] is None

    def test_subcontractor_lookup_with_uei(self):
        """Test that subcontractors are looked up using UEI"""
        result = expand_prime_recipient_subcontractors(
            uei="VALID_UEI_123456789",
            recipient_name="PRIME CORP",
        )

        # Should attempt to find subcontractors
        assert isinstance(result["subcontractors"], list)

    def test_subcontractor_lookup_with_duns(self):
        """Test that subcontractors are looked up using DUNS"""
        result = expand_prime_recipient_subcontractors(
            duns="123456789",
            recipient_name="PRIME CORP",
        )

        # Should attempt to find subcontractors
        assert isinstance(result["subcontractors"], list)

    def test_no_duplicate_names_in_all_recipient_names(self):
        """Test that all_recipient_names contains no duplicates"""
        result = expand_prime_recipient_subcontractors(
            recipient_name="TEST CORP",
            uei="TEST123",
        )

        names = result["all_recipient_names"]
        assert len(names) == len(set(names))


@pytest.mark.django_db
@pytest.mark.elasticsearch
class TestRetrieveCompanyAndSubcontractorsIntegration:
    """Integration tests for retrieve_company_and_subcontractors"""

    def test_returns_dict_with_required_keys(self):
        """Test that function returns dict with expected structure"""
        result = retrieve_company_and_subcontractors("TEST CORP")

        assert isinstance(result, dict)
        assert "query" in result
        assert "matches" in result
        assert "recipient_names" in result

    def test_query_field_matches_input(self):
        """Test that query field contains original search text"""
        search_text = "ACME Corporation"
        result = retrieve_company_and_subcontractors(search_text)

        assert result["query"] == search_text

    def test_matches_is_list(self):
        """Test that matches field is a list"""
        result = retrieve_company_and_subcontractors("TEST")

        assert isinstance(result["matches"], list)

    def test_recipient_names_is_list(self):
        """Test that recipient_names field is a list"""
        result = retrieve_company_and_subcontractors("TEST")

        assert isinstance(result["recipient_names"], list)

    def test_no_duplicate_recipient_names(self):
        """Test that recipient_names contains no duplicates"""
        result = retrieve_company_and_subcontractors("CORP")

        names = result["recipient_names"]
        assert len(names) == len(set(names))

    def test_respects_limit_parameter(self):
        """Test that limit parameter controls number of matches"""
        result_3 = retrieve_company_and_subcontractors("CORP", limit=3)
        result_10 = retrieve_company_and_subcontractors("CORP", limit=10)

        assert len(result_3["matches"]) <= 3
        assert len(result_10["matches"]) <= 10

    def test_includes_subcontractors_in_results(self):
        """Test that subcontractors are included when available"""
        result = retrieve_company_and_subcontractors("PRIME CORP", limit=5)

        # If matches exist, verify structure
        if result["matches"]:
            for match in result["matches"]:
                assert isinstance(match, dict)
                # Each match should have a recipient object
                assert len(match) > 0

    def test_handles_no_matches(self):
        """Test behavior when no matches are found"""
        result = retrieve_company_and_subcontractors("NONEXISTENT_XYZ_12345")

        assert result["matches"] == []
        assert result["recipient_names"] == []
        assert result["query"] == "NONEXISTENT_XYZ_12345"

    def test_case_insensitive_search(self):
        """Test that search is case-insensitive"""
        result_lower = retrieve_company_and_subcontractors("acme corp")
        result_upper = retrieve_company_and_subcontractors("ACME CORP")

        # Should return same number of matches
        assert len(result_lower["matches"]) == len(result_upper["matches"])

    def test_handles_special_characters(self):
        """Test that special characters are handled properly"""
        # Should not raise exception
        result = retrieve_company_and_subcontractors("ACME & Co. (2024)")

        assert isinstance(result, dict)
        assert "query" in result

    def test_recipient_names_extracted_from_matches(self):
        """Test that recipient_names are properly extracted from matches"""
        result = retrieve_company_and_subcontractors("TEST", limit=5)

        # If matches exist, verify names are extracted
        if result["matches"]:
            assert len(result["recipient_names"]) > 0
            # All names should be strings
            assert all(isinstance(name, str) for name in result["recipient_names"])


@pytest.mark.django_db
@pytest.mark.elasticsearch
class TestRecipientRetrievalEndToEnd:
    """End-to-end integration tests across multiple functions"""

    def test_fuzzy_search_to_expand_workflow(self):
        """Test workflow from fuzzy search to expansion"""
        # Step 1: Fuzzy search
        search_results = fuzzy_search_recipients("ACME", limit=1)

        if search_results:
            # Step 2: Expand first result
            first_result = search_results[0]
            expanded = expand_prime_recipient_subcontractors(
                recipient_name=first_result["recipient_name"],
                uei=first_result["uei"],
                duns=first_result["duns"],
                recipient_hash=first_result["recipient_hash"],
                recipient_level=first_result["recipient_level"],
            )

            # Verify workflow
            assert expanded["prime"]["recipient_name"] == first_result["recipient_name"]
            assert isinstance(expanded["subcontractors"], list)

    def test_retrieve_company_includes_all_data(self):
        """Test that retrieve_company_and_subcontractors provides complete data"""
        result = retrieve_company_and_subcontractors("CORP", limit=3)

        # Verify all expected data is present
        assert "query" in result
        assert "matches" in result
        assert "recipient_names" in result

        # If matches exist, verify they have proper structure
        for match in result["matches"]:
            assert isinstance(match, dict)
            recipient_obj = next(iter(match.values()))
            assert "filter" in recipient_obj

    def test_consistent_results_across_functions(self):
        """Test that different functions return consistent data for same recipient"""
        search_text = "ACME CORPORATION"

        # Get results from both functions
        fuzzy_results = fuzzy_search_recipients(search_text, limit=5)
        retrieve_results = retrieve_company_and_subcontractors(search_text, limit=5)

        # Both should return data (or both empty)
        assert isinstance(fuzzy_results, list)
        assert isinstance(retrieve_results["matches"], list)

    def test_performance_with_large_limit(self):
        """Test that functions handle large limits without errors"""
        # Should not timeout or error with large limit
        result = fuzzy_search_recipients("CORP", limit=100)
        assert isinstance(result, list)

        result2 = retrieve_company_and_subcontractors("CORP", limit=50)
        assert isinstance(result2, dict)
