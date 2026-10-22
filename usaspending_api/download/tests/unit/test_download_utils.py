"""
Unit tests for usaspending_api/download/download_utils.py

Focus: SEARCH_DOWNLOAD_NAME_BY_SPENDING_LEVEL, DOWNLOAD_TYPE_TO_SPENDING_LEVEL,
_resolve_search_spending_levels, create_search_download_name, and the "search"
branch of create_unique_filename.
"""

import re
from unittest.mock import patch

import pytest

from usaspending_api.download.download_utils import (
    DOWNLOAD_TYPE_TO_SPENDING_LEVEL,
    SEARCH_DOWNLOAD_NAME_BY_SPENDING_LEVEL,
    _resolve_search_spending_levels,
    create_search_download_name,
    create_unique_filename,
)

# Matches "%Y-%m-%d_H%HM%MS%S%f" e.g. 2026-09-14_H10M30S05123456
TIMESTAMP_PATTERN = r"\d{4}-\d{2}-\d{2}_H\d{2}M\d{2}S\d{2}\d{6}"


class TestResolveSearchSpendingLevels:
    """_resolve_search_spending_levels() resolution priority and derivation logic."""

    def test_explicit_spending_level_is_used_verbatim(self):
        """spending_level key takes precedence over download_types when truthy."""
        json_request = {
            "spending_level": ["awards", "subawards"],
            "download_types": ["elasticsearch_transactions"],  # should be ignored
        }
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards", "subawards"]

    def test_explicit_spending_level_empty_list_falls_through_to_derivation(self):
        """Empty list is falsy, so behavior falls through to download_types derivation."""
        json_request = {
            "spending_level": [],
            "download_types": ["elasticsearch_awards"],
        }
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards"]

    @pytest.mark.parametrize(
        "download_type,expected_level",
        [
            ("elasticsearch_awards", "awards"),
            ("elasticsearch_transactions", "transactions"),
            ("elasticsearch_sub_awards", "subawards"),
        ],
    )
    def test_single_known_download_type_maps_correctly(self, download_type, expected_level):
        json_request = {"download_types": [download_type]}
        result = _resolve_search_spending_levels(json_request)
        assert result == [expected_level]

    def test_multiple_known_download_types_preserve_order(self):
        json_request = {
            "download_types": ["elasticsearch_transactions", "elasticsearch_awards"],
        }
        result = _resolve_search_spending_levels(json_request)
        assert result == ["transactions", "awards"]

    def test_unknown_download_type_is_filtered_out(self):
        """download_types not present in DOWNLOAD_TYPE_TO_SPENDING_LEVEL are silently dropped."""
        json_request = {
            "download_types": ["elasticsearch_awards", "some_unmapped_type"],
        }
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards"]

    def test_all_unknown_download_types_yields_default_fallback(self):
        """If every download_type is unmapped, `derived` is empty, so default triple applies."""
        json_request = {"download_types": ["totally_unknown"]}
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards", "transactions", "subawards"]

    def test_missing_download_types_key_yields_default_fallback(self):
        json_request = {}
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards", "transactions", "subawards"]

    def test_none_download_types_yields_default_fallback(self):
        """`json_request.get('download_types') or ()` guards against explicit None."""
        json_request = {"download_types": None}
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards", "transactions", "subawards"]

    def test_empty_download_types_list_yields_default_fallback(self):
        json_request = {"download_types": []}
        result = _resolve_search_spending_levels(json_request)
        assert result == ["awards", "transactions", "subawards"]


class TestDownloadTypeToSpendingLevelMapping:
    """Guard against silent breakage if the mapping dict is edited."""

    def test_expected_keys_present(self):
        assert set(DOWNLOAD_TYPE_TO_SPENDING_LEVEL.keys()) == {
            "elasticsearch_awards",
            "elasticsearch_transactions",
            "elasticsearch_sub_awards",
        }

    def test_expected_values_present(self):
        assert set(DOWNLOAD_TYPE_TO_SPENDING_LEVEL.values()) == {
            "awards",
            "transactions",
            "subawards",
        }


class TestSearchDownloadNameBySpendingLevelMapping:
    """Guard against typos/missing entries in the lookup dict itself."""

    @pytest.mark.parametrize(
        "levels,expected_name",
        [
            (("awards",), "PrimeAwardSummaries"),
            (("transactions",), "PrimeTransactions"),
            (("subawards",), "SubawardSummaries"),
            (("awards", "subawards"), "PrimeAwardSummariesAndSubawards"),
            (("subawards", "transactions"), "PrimeTransactionsAndSubawards"),
            (("awards", "transactions"), "PrimeAwardSummariesAndTransactions"),
            (("awards", "subawards", "transactions"), "PrimeAwardTransactionsAndSubawards"),
        ],
    )
    def test_known_spending_level_combos_resolve(self, levels, expected_name):
        assert SEARCH_DOWNLOAD_NAME_BY_SPENDING_LEVEL[levels] == expected_name


class TestCreateSearchDownloadName:
    """create_search_download_name() end-to-end name resolution."""

    def test_single_level_awards(self):
        json_request = {"spending_level": ["awards"]}
        assert create_search_download_name(json_request) == "PrimeAwardSummaries"

    def test_single_level_transactions(self):
        json_request = {"spending_level": ["transactions"]}
        assert create_search_download_name(json_request) == "PrimeTransactions"

    def test_single_level_subawards(self):
        json_request = {"spending_level": ["subawards"]}
        assert create_search_download_name(json_request) == "SubawardSummaries"

    def test_double_level_sorted_regardless_of_input_order(self):
        """Sorting means input order doesn't affect the lookup key."""
        json_request_a = {"spending_level": ["subawards", "awards"]}
        json_request_b = {"spending_level": ["awards", "subawards"]}
        assert (
            create_search_download_name(json_request_a)
            == create_search_download_name(json_request_b)
            == "PrimeAwardSummariesAndSubawards"
        )

    def test_derived_from_download_types_single(self):
        json_request = {"download_types": ["elasticsearch_awards"]}
        assert create_search_download_name(json_request) == "PrimeAwardSummaries"

    def test_derived_from_download_types_double(self):
        json_request = {
            "download_types": ["elasticsearch_transactions", "elasticsearch_sub_awards"],
        }
        assert create_search_download_name(json_request) == "PrimeTransactionsAndSubawards"

    def test_default_triple_level_resolves_to_combined_name(self):
        """
        No explicit spending_level and no download_types triggers the
        default triple-level fallback in _resolve_search_spending_levels(),
        which resolves correctly against SEARCH_DOWNLOAD_NAME_BY_SPENDING_LEVEL.
        """
        json_request = {}
        assert create_search_download_name(json_request) == "PrimeAwardTransactionsAndSubawards"

    def test_unknown_download_type_falls_back_to_default_triple_and_resolves(self):
        """Unmapped download_types fall back to the default triple, which resolves correctly."""
        json_request = {"download_types": ["not_a_real_type"]}
        assert create_search_download_name(json_request) == "PrimeAwardTransactionsAndSubawards"


class TestCreateUniqueFilenameSearchBranch:
    """Integration coverage of the 'search' request_type branch in create_unique_filename()."""

    @pytest.mark.parametrize(
        "json_request,expected_prefix",
        [
            pytest.param(
                {"request_type": "search", "spending_level": ["awards"]},
                "PrimeAwardSummaries",
                id="single-level-awards-via-spending_level",
            ),
            pytest.param(
                {"request_type": "search", "spending_level": ["transactions"]},
                "PrimeTransactions",
                id="single-level-transactions-via-spending_level",
            ),
            pytest.param(
                {"request_type": "search", "spending_level": ["awards", "transactions"]},
                "PrimeAwardSummariesAndTransactions",
                id="double-level-awards-and-transactions",
            ),
            pytest.param(
                {"request_type": "search", "download_types": ["elasticsearch_sub_awards"]},
                "SubawardSummaries",
                id="single-level-subawards-derived-from-download_types",
            ),
            pytest.param(
                {
                    "request_type": "search",
                    "download_types": ["elasticsearch_transactions", "elasticsearch_sub_awards"],
                },
                "PrimeTransactionsAndSubawards",
                id="double-level-derived-from-download_types",
            ),
            pytest.param(
                {"request_type": "search"},
                "PrimeAwardTransactionsAndSubawards",
                id="default-triple-level-no-spending_level-no-download_types",
            ),
        ],
    )
    def test_search_filename_has_correct_prefix_and_extension(self, json_request, expected_prefix):
        result = create_unique_filename(json_request)
        assert result.startswith(f"{expected_prefix}_")
        assert result.endswith(".zip")

    @pytest.mark.parametrize(
        "json_request",
        [
            pytest.param({"request_type": "search", "spending_level": ["awards"]}, id="awards"),
            pytest.param({"request_type": "search", "spending_level": ["transactions"]}, id="transactions"),
            pytest.param({"request_type": "search", "spending_level": ["subawards"]}, id="subawards"),
        ],
    )
    def test_search_filename_embeds_valid_timestamp_format(self, json_request):
        result = create_unique_filename(json_request)
        match = re.search(TIMESTAMP_PATTERN, result)
        assert match is not None, f"Expected timestamp pattern in filename, got: {result}"

    @patch("usaspending_api.download.download_utils.datetime")
    def test_search_filename_uses_frozen_timestamp(self, mock_datetime):
        """Verify exact timestamp formatting by mocking datetime.now()."""
        from datetime import datetime as real_datetime
        from datetime import timezone as real_timezone

        frozen_time = real_datetime(2026, 9, 14, 10, 30, 5, 123456, tzinfo=real_timezone.utc)
        mock_datetime.now.return_value = frozen_time
        mock_datetime.strftime = real_datetime.strftime

        json_request = {
            "request_type": "search",
            "spending_level": ["subawards"],
        }
        result = create_unique_filename(json_request)
        assert result == "SubawardSummaries_2026-09-14_H10M30S05123456.zip"
