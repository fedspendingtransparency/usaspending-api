from unittest.mock import Mock, patch

from usaspending_api.llm.tests.helper import (
    build_fuzzy_recipient_query,
    fuzzy_search_recipients,
    retrieve_recipient_names,
)
from usaspending_api.llm.tools.lookup_recipient import RecipientLookupTool


def _make_response(hits: list[dict]) -> Mock:
    mock_hits = []
    for hit in hits:
        mock_hit = Mock()
        mock_hit.to_dict.return_value = hit
        mock_hits.append(mock_hit)
    response = Mock()
    response.hits = mock_hits
    return response


class TestMatchedIdentifier:
    """Tests for RecipientLookupTool._matched_identifier"""

    def test_matches_uei(self):
        hit = {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"}
        assert RecipientLookupTool._matched_identifier(hit, "UEI123456789") == "UEI123456789"

    def test_matches_duns(self):
        hit = {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"}
        assert RecipientLookupTool._matched_identifier(hit, "123456789") == "123456789"

    def test_matches_hash_case_insensitively(self):
        hit = {"recipient_name": "ACME CORP", "recipient_hash": "abc-123-def"}
        # Query arrives uppercased; the stored hash is lowercase but should still match.
        assert RecipientLookupTool._matched_identifier(hit, "ABC-123-DEF") == "abc-123-def"

    def test_returns_none_when_only_name_matches(self):
        hit = {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"}
        assert RecipientLookupTool._matched_identifier(hit, "ACME CORP") is None

    def test_ignores_missing_identifiers(self):
        hit = {"recipient_name": "ACME CORP"}
        assert RecipientLookupTool._matched_identifier(hit, "UEI123456789") is None


class TestExtractRecipientNames:
    """Tests for RecipientLookupTool._extract_recipient_names identifier/exact-match behavior"""

    def test_exact_uei_match_returns_only_that_identifier(self):
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"},
                {"recipient_name": "ACME INDUSTRIES", "uei": "UEI999", "duns": "999"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "UEI123456789")
        assert result == {"recipient_names": ["UEI123456789"]}

    def test_exact_duns_match_returns_only_that_identifier(self):
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"},
                {"recipient_name": "ACME INDUSTRIES", "uei": "UEI999", "duns": "999"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "123456789")
        assert result == {"recipient_names": ["123456789"]}

    def test_exact_hash_match_returns_only_that_identifier(self):
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "recipient_hash": "hash123"},
                {"recipient_name": "ACME INDUSTRIES", "recipient_hash": "hash789"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "HASH123")
        assert result == {"recipient_names": ["hash123"]}

    def test_exact_name_match_still_returns_all_fuzzy_matches(self):
        """An exact name match isn't short-circuited since a different match may rank better."""
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"},
                {"recipient_name": "ACME CORPORATION", "uei": "UEI999", "duns": "999"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "ACME CORP")
        assert result == {"recipient_names": ["ACME CORP", "ACME CORPORATION"]}

    def test_returns_all_names_when_no_exact_match(self):
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"},
                {"recipient_name": "ACME INDUSTRIES", "uei": "UEI999", "duns": "999"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "ACME")
        assert result == {"recipient_names": ["ACME CORP", "ACME INDUSTRIES"]}

    def test_deduplicates_fuzzy_names(self):
        response = _make_response(
            [
                {"recipient_name": "ACME CORP", "uei": "UEI123456789", "duns": "123456789"},
                {"recipient_name": "ACME CORP", "uei": "UEI999", "duns": "999"},
            ]
        )
        result = RecipientLookupTool()._extract_recipient_names(response, "ACME")
        assert result == {"recipient_names": ["ACME CORP"]}


class TestBuildFuzzyRecipientQuery:
    """Tests for build_fuzzy_recipient_query function"""

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
    def test_builds_query_with_sanitized_text(self, mock_sanitize, mock_tool):
        """Test that query is built with sanitized and uppercase text"""
        mock_sanitize.return_value = "acme corp"
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        result = build_fuzzy_recipient_query("  ACME Corp  ")

        mock_sanitize.assert_called_once_with("  ACME Corp  ")
        mock_tool._build_search.assert_called_once_with("ACME CORP", top_k=10)
        assert result == mock_search

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
    def test_strips_whitespace(self, mock_sanitize, mock_tool):
        """Test that whitespace is stripped from search text"""
        mock_sanitize.return_value = "  test  "
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        build_fuzzy_recipient_query("  test  ")

        mock_tool._build_search.assert_called_once_with("TEST", top_k=10)

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
    def test_converts_to_uppercase(self, mock_sanitize, mock_tool):
        """Test that search text is converted to uppercase"""
        mock_sanitize.return_value = "lowercase text"
        mock_search = Mock()
        mock_tool._build_search.return_value = mock_search

        build_fuzzy_recipient_query("lowercase text")

        mock_tool._build_search.assert_called_once_with("LOWERCASE TEXT", top_k=10)


class TestFuzzySearchRecipients:
    """Tests for fuzzy_search_recipients function"""

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
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

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
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

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
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

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
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

    @patch("usaspending_api.llm.tests.helper._tool")
    @patch("usaspending_api.llm.tests.helper.es_sanitize")
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


class TestRetrieveRecipientNames:
    """Tests for retrieve_recipient_names function"""

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_returns_list_of_recipient_names(self, mock_tool):
        """Test that function returns a list of recipient names"""
        mock_tool.lookup_recipient.return_value = [
            "ACME CORP",
            "UEI123456789",
            "123456789",
        ]

        result = retrieve_recipient_names("acme")

        mock_tool.lookup_recipient.assert_called_once_with("acme", top_k=5)
        assert result is not None, "Function returned None"
        assert isinstance(result, list), f"Expected list, got {type(result)}"
        assert result == ["ACME CORP", "UEI123456789", "123456789"]

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_handles_no_results(self, mock_tool):
        """Test when no results are returned"""
        mock_tool.lookup_recipient.return_value = []

        result = retrieve_recipient_names("nonexistent")

        assert isinstance(result, list)
        assert result == []

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_respects_limit_parameter(self, mock_tool):
        """Test that limit parameter is passed to lookup_recipient"""
        mock_tool.lookup_recipient.return_value = []

        retrieve_recipient_names("test", limit=10)

        mock_tool.lookup_recipient.assert_called_once_with("test", top_k=10)

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_uses_default_limit(self, mock_tool):
        """Test that default limit is used when not specified"""
        mock_tool.lookup_recipient.return_value = []

        retrieve_recipient_names("test")

        mock_tool.lookup_recipient.assert_called_once_with("test", top_k=5)

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_returns_deduplicated_list(self, mock_tool):
        """Test that lookup_recipient returns deduplicated results"""
        mock_tool.lookup_recipient.return_value = [
            "ACME CORP",
            "UEI123",
            "456",  # No duplicates in return value
        ]

        result = retrieve_recipient_names("acme")

        assert isinstance(result, list)
        assert len(result) == 3
        assert "ACME CORP" in result
        assert "UEI123" in result
        assert "456" in result

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_handles_multiple_recipients(self, mock_tool):
        """Test handling multiple recipient identifiers"""
        mock_tool.lookup_recipient.return_value = [
            "ACME CORP",
            "UEI111",
            "111",
            "BETA CORP",
            "UEI222",
            "222",
        ]

        result = retrieve_recipient_names("corp")

        assert isinstance(result, list)
        assert len(result) == 6
        assert "ACME CORP" in result
        assert "BETA CORP" in result
        assert "UEI111" in result
        assert "UEI222" in result

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_passes_search_text_unchanged(self, mock_tool):
        """Test that search text is passed to lookup_recipient unchanged"""
        mock_tool.lookup_recipient.return_value = []

        retrieve_recipient_names("  Test Query  ", limit=10)

        # Should pass the search text as-is (lookup_recipient handles sanitization)
        mock_tool.lookup_recipient.assert_called_once_with("  Test Query  ", top_k=10)

    @patch("usaspending_api.llm.tests.helper._tool")
    def test_returns_empty_list_on_none(self, mock_tool):
        """Test handling when lookup_recipient returns None"""
        mock_tool.lookup_recipient.return_value = None

        result = retrieve_recipient_names("test")

        # Should handle None gracefully
        assert result is None or result == []
