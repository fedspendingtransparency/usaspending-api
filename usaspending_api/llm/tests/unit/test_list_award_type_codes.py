from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings, award_type_mapping
from usaspending_api.llm.tools.list_award_type_codes import (
    AWARD_TYPE_CODES,
    list_award_type_codes,
    list_award_type_codes_tool,
)


def _all_entries() -> list[dict[str, str]]:
    return [entry for entries in AWARD_TYPE_CODES.values() for entry in entries]


class TestAwardTypeCodesData:
    """Tests for the static AWARD_TYPE_CODES catalog"""

    def test_covers_every_award_type_exactly(self):
        """The tool offers exactly the codes the filter accepts — no more, no less."""
        catalog_codes = [entry["code"] for entry in _all_entries()]

        assert sorted(catalog_codes) == sorted(award_type_mapping)
        assert len(catalog_codes) == len(set(catalog_codes))  # no duplicates across groups

    def test_grouping_matches_the_validation_partition(self):
        """Codes are grouped exactly as the single-group validator partitions them.

        Group *names* mirror the API error response (direct_payments/other_financial_assistance are
        swapped relative to the internal variable names), so we compare code membership rather than
        names.
        """
        tool_partition = {frozenset(entry["code"] for entry in entries) for entries in AWARD_TYPE_CODES.values()}
        validation_partition = {frozenset(codes) for codes in all_award_types_mappings.values()}

        assert tool_partition == validation_partition

    def test_every_entry_has_code_and_nonempty_label(self):
        for entry in _all_entries():
            assert set(entry.keys()) == {"code", "label"}
            assert entry["code"]
            assert entry["label"]


class TestListAwardTypeCodesFunction:
    """Tests for the list_award_type_codes function"""

    def test_returns_the_catalog(self):
        assert list_award_type_codes() is AWARD_TYPE_CODES

    def test_callable_with_no_input(self):
        """The assistant invokes tools as function(**tool_input); this tool takes no input."""
        assert list_award_type_codes_tool.function(**{}) is AWARD_TYPE_CODES


class TestListAwardTypeCodesTool:
    """Tests for the AITool wiring"""

    def test_tool_name(self):
        assert list_award_type_codes_tool.description.name == "list_award_type_codes"

    def test_input_schema_accepts_no_arguments(self):
        schema = list_award_type_codes_tool.description.input_schema
        assert schema["properties"] == {}
        assert schema["required"] == []

    def test_logging_ignores_empty_input(self):
        assert list_award_type_codes_tool.logging({}) == "Listing award type codes."
