"""Unit tests for recipient_retrieval module"""
from unittest.mock import Mock, patch

from usaspending_api.llm.retrieval.recipient_retrieval import (
    build_fuzzy_recipient_query,
    expand_prime_recipient_subcontractors,
    fuzzy_search_recipients,
    retrieve_company_and_subcontractors,
)


class TestBuildFuzzyRecipientQuery:
    """Tests for build_fuzzy_recipient_query function"""

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_builds_query_with_sanitized_text(self, mock_sanitize, mock_tool):
        """Test that query is built with sanitized and uppercase text"""
        mock_sanitize.return_value = "acme corp"
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        result = build_fuzzy_recipient_query("  ACME Corp  ")

        mock_sanitize.assert_called_once_with("  ACME Corp  ")
        mock_tool._build_search.assert_called_once_with("ACME CORP", top_k=10)
        assert result == mock_search

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_strips_whitespace(self, mock_sanitize, mock_tool):
        """Test that whitespace is stripped from search text"""
        mock_sanitize.return_value = "  test  "
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        build_fuzzy_recipient_query("  test  ")

        mock_tool._build_search.assert_called_once_with("TEST", top_k=10)

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_converts_to_uppercase(self, mock_sanitize, mock_tool):
        """Test that search text is converted to uppercase"""
        mock_sanitize.return_value = "lowercase text"
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        build_fuzzy_recipient_query("lowercase text")

        mock_tool._build_search.assert_called_once_with("LOWERCASE TEXT", top_k=10)


class TestFuzzySearchRecipients:
    """Tests for fuzzy_search_recipients function"""

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_returns_empty_list_when_no_hits(self, mock_sanitize, mock_tool):
        """Test that empty list is returned when no hits found"""
        mock_sanitize.return_value = "test"
        mock_response = Mock()
        mock_response.hits = []
        mock_search = Mock()
        mock_search.handle_execute.return_value = mock_response
        mock_tool._build_search.return_value = mock_search

        result = fuzzy_search_recipients("test")

        assert result == []

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_returns_formatted_results_with_hits(self, mock_sanitize, mock_tool):
        """Test that results are properly formatted when hits exist"""
        mock_sanitize.return_value = "acme"

        # Create mock hit
        mock_hit = Mock()
        mock_hit.to_dict.return_value = {
            "recipient_name": "ACME CORP",
            "uei": "UEI123456789",
            "duns": "123456789",
            "recipient_level": "P",
            "recipient_hash": "hash123",
        }
        mock_hit.meta.score = 0.95

        mock_response = Mock()
        mock_response.hits = [mock_hit]
        mock_search = Mock()
        mock_search.handle_execute.return_value = mock_response
        mock_tool._build_search.return_value = mock_search

        result = fuzzy_search_recipients("acme", limit=5)

        assert len(result) == 1
        assert result[0]["recipient_name"] == "ACME CORP"
        assert result[0]["uei"] == "UEI123456789"
        assert result[0]["duns"] == "123456789"
        assert result[0]["recipient_level"] == "P"
        assert result[0]["recipient_hash"] == "hash123"
        assert result[0]["score"] == 0.95
        mock_tool._build_search.assert_called_once_with("ACME", top_k=5)

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_handles_multiple_hits(self, mock_sanitize, mock_tool):
        """Test that multiple hits are properly formatted"""
        mock_sanitize.return_value = "corp"

        # Create multiple mock hits
        mock_hit1 = Mock()
        mock_hit1.to_dict.return_value = {
            "recipient_name": "ACME CORP",
            "uei": "UEI111",
            "duns": "111",
            "recipient_level": "P",
            "recipient_hash": "hash1",
        }
        mock_hit1.meta.score = 0.95

        mock_hit2 = Mock()
        mock_hit2.to_dict.return_value = {
            "recipient_name": "BETA CORP",
            "uei": "UEI222",
            "duns": "222",
            "recipient_level": "C",
            "recipient_hash": "hash2",
        }
        mock_hit2.meta.score = 0.85

        mock_response = Mock()
        mock_response.hits = [mock_hit1, mock_hit2]
        mock_search = Mock()
        mock_search.handle_execute.return_value = mock_response
        mock_tool._build_search.return_value = mock_search

        result = fuzzy_search_recipients("corp")

        assert len(result) == 2
        assert result[0]["recipient_name"] == "ACME CORP"
        assert result[1]["recipient_name"] == "BETA CORP"

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_handles_missing_fields_in_hits(self, mock_sanitize, mock_tool):
        """Test that missing fields are handled gracefully"""
        mock_sanitize.return_value = "test"

        mock_hit = Mock()
        mock_hit.to_dict.return_value = {
            "recipient_name": "TEST CORP",
            # Missing uei, duns, etc.
        }
        mock_hit.meta.score = 0.5

        mock_response = Mock()
        mock_response.hits = [mock_hit]
        mock_search = Mock()
        mock_search.handle_execute.return_value = mock_response
        mock_tool._build_search.return_value = mock_search

        result = fuzzy_search_recipients("test")

        assert len(result) == 1
        assert result[0]["recipient_name"] == "TEST CORP"
        assert result[0]["uei"] is None
        assert result[0]["duns"] is None
        assert result[0]["recipient_level"] is None
        assert result[0]["recipient_hash"] is None
        assert result[0]["score"] == 0.5

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    @patch("usaspending_api.llm.retrieval.recipient_retrieval.es_sanitize")
    def test_respects_limit_parameter(self, mock_sanitize, mock_tool):
        """Test that limit parameter is passed to _build_search"""
        mock_sanitize.return_value = "test"
        mock_response = Mock()
        mock_response.hits = []
        mock_search = Mock()
        mock_search.handle_execute.return_value = mock_response
        mock_tool._build_search.return_value = mock_search

        fuzzy_search_recipients("test", limit=25)

        mock_tool._build_search.assert_called_once_with("TEST", top_k=25)


class TestExpandPrimeRecipientSubcontractors:
    """Tests for expand_prime_recipient_subcontractors function"""

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_expands_with_all_fields(self, mock_tool):
        """Test expansion with all prime recipient fields"""
        mock_tool._get_subcontractors.return_value = [
            {"recipient_name": "SUBCONTRACTOR A"},
            {"recipient_name": "SUBCONTRACTOR B"},
        ]

        result = expand_prime_recipient_subcontractors(
            recipient_name="PRIME CORP",
            uei="UEI123",
            duns="123456789",
            recipient_hash="hash123",
            recipient_level="P",
        )

        assert result["prime"]["recipient_name"] == "PRIME CORP"
        assert result["prime"]["uei"] == "UEI123"
        assert result["prime"]["duns"] == "123456789"
        assert result["prime"]["recipient_hash"] == "hash123"
        assert result["prime"]["recipient_level"] == "P"
        assert len(result["subcontractors"]) == 2
        assert result["all_recipient_names"] == ["PRIME CORP", "SUBCONTRACTOR A", "SUBCONTRACTOR B"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_no_subcontractors(self, mock_tool):
        """Test when no subcontractors are found"""
        mock_tool._get_subcontractors.return_value = []

        result = expand_prime_recipient_subcontractors(
            recipient_name="PRIME CORP",
            uei="UEI123",
        )

        assert result["prime"]["recipient_name"] == "PRIME CORP"
        assert result["subcontractors"] == []
        assert result["all_recipient_names"] == ["PRIME CORP"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_deduplicates_recipient_names(self, mock_tool):
        """Test that duplicate recipient names are removed"""
        mock_tool._get_subcontractors.return_value = [
            {"recipient_name": "PRIME CORP"},  # Duplicate of prime
            {"recipient_name": "SUBCONTRACTOR A"},
            {"recipient_name": "SUBCONTRACTOR A"},  # Duplicate
        ]

        result = expand_prime_recipient_subcontractors(
            recipient_name="PRIME CORP",
            uei="UEI123",
        )

        assert result["all_recipient_names"] == ["PRIME CORP", "SUBCONTRACTOR A"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_missing_prime_name(self, mock_tool):
        """Test when prime recipient has no name"""
        mock_tool._get_subcontractors.return_value = [
            {"recipient_name": "SUBCONTRACTOR A"},
        ]

        result = expand_prime_recipient_subcontractors(
            uei="UEI123",
            duns="123456789",
        )

        assert result["prime"]["recipient_name"] is None
        assert result["all_recipient_names"] == ["SUBCONTRACTOR A"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_subcontractors_without_names(self, mock_tool):
        """Test subcontractors with missing or None names"""
        mock_tool._get_subcontractors.return_value = [
            {"recipient_name": None},
            {"recipient_name": ""},
            {"recipient_name": "VALID SUBCONTRACTOR"},
            {},  # No recipient_name key
        ]

        result = expand_prime_recipient_subcontractors(
            recipient_name="PRIME CORP",
            uei="UEI123",
        )

        assert result["all_recipient_names"] == ["PRIME CORP", "VALID SUBCONTRACTOR"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_calls__get_subcontractors_with_correct_params(self, mock_tool):
        """Test that _get_subcontractors is called with correct parameters"""
        mock_tool._get_subcontractors.return_value = []

        expand_prime_recipient_subcontractors(
            recipient_name="PRIME CORP",
            uei="UEI123",
            duns="123456789",
        )

        mock_tool._get_subcontractors.assert_called_once_with(
            uei="UEI123",
            duns="123456789",
            recipient_name="PRIME CORP",
        )


class TestRetrieveCompanyAndSubcontractors:
    """Tests for retrieve_company_and_subcontractors function"""

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_retrieves_with_default_limit(self, mock_tool):
        """Test retrieval with default limit"""
        mock_tool.lookup_recipient.return_value = {
            "results": [
                {
                    "recipient": {
                        "filter": {"recipient_search_text": ["ACME CORP", "UEI123"]},
                    }
                }
            ]
        }

        result = retrieve_company_and_subcontractors("acme")

        mock_tool.lookup_recipient.assert_called_once_with(
            "acme",
            include_subcontractors=True,
            top_k=5,
        )
        assert result["query"] == "acme"
        assert len(result["matches"]) == 1
        assert result["recipient_names"] == ["ACME CORP", "UEI123"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_retrieves_with_custom_limit(self, mock_tool):
        """Test retrieval with custom limit"""
        mock_tool.lookup_recipient.return_value = {"results": []}

        retrieve_company_and_subcontractors("test", limit=10)

        mock_tool.lookup_recipient.assert_called_once_with(
            "test",
            include_subcontractors=True,
            top_k=10,
        )

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_no_results(self, mock_tool):
        """Test when no results are returned"""
        mock_tool.lookup_recipient.return_value = {"results": []}

        result = retrieve_company_and_subcontractors("nonexistent")

        assert result["query"] == "nonexistent"
        assert result["matches"] == []
        assert result["recipient_names"] == []

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_multiple_matches(self, mock_tool):
        """Test with multiple matching recipients"""
        mock_tool.lookup_recipient.return_value = {
            "results": [
                {
                    "recipient1": {
                        "filter": {"recipient_search_text": ["ACME CORP", "UEI111"]},
                    }
                },
                {
                    "recipient2": {
                        "filter": {"recipient_search_text": ["BETA CORP", "UEI222"]},
                    }
                },
            ]
        }

        result = retrieve_company_and_subcontractors("corp")

        assert len(result["matches"]) == 2
        assert result["recipient_names"] == ["ACME CORP", "UEI111", "BETA CORP", "UEI222"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_deduplicates_recipient_names(self, mock_tool):
        """Test that duplicate recipient names are removed while preserving order"""
        mock_tool.lookup_recipient.return_value = {
            "results": [
                {
                    "recipient1": {
                        "filter": {"recipient_search_text": ["ACME CORP", "UEI123", "ACME CORP"]},
                    }
                },
                {
                    "recipient2": {
                        "filter": {"recipient_search_text": ["UEI123", "BETA CORP"]},
                    }
                },
            ]
        }

        result = retrieve_company_and_subcontractors("corp")

        # Should preserve order and remove duplicates
        assert result["recipient_names"] == ["ACME CORP", "UEI123", "BETA CORP"]

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_missing_filter_field(self, mock_tool):
        """Test when filter field is missing from results"""
        mock_tool.lookup_recipient.return_value = {
            "results": [
                {
                    "recipient1": {
                        "other_field": "value",
                    }
                }
            ]
        }

        result = retrieve_company_and_subcontractors("test")

        assert result["recipient_names"] == []

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_empty_recipient_search_text(self, mock_tool):
        """Test when recipient_search_text is empty"""
        mock_tool.lookup_recipient.return_value = {
            "results": [
                {
                    "recipient1": {
                        "filter": {"recipient_search_text": []},
                    }
                }
            ]
        }

        result = retrieve_company_and_subcontractors("test")

        assert result["recipient_names"] == []

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_handles_missing_results_key(self, mock_tool):
        """Test when results key is missing from response"""
        mock_tool.lookup_recipient.return_value = {}

        result = retrieve_company_and_subcontractors("test")

        assert result["query"] == "test"
        assert result["matches"] == []
        assert result["recipient_names"] == []

    @patch("usaspending_api.llm.retrieval.recipient_retrieval._tool")
    def test_preserves_query_text(self, mock_tool):
        """Test that original query text is preserved in result"""
        mock_tool.lookup_recipient.return_value = {"results": []}

        result = retrieve_company_and_subcontractors("  Original Query Text  ")

        assert result["query"] == "  Original Query Text  "
