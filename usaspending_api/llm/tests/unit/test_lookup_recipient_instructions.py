import pytest

from usaspending_api.llm.tools.lookup_recipient import (
    IDENTIFIER_MATCH_INSTRUCTIONS,
    NO_RECIPIENT_INSTRUCTIONS,
    RECIPIENT_NAMES_INSTRUCTIONS,
    RecipientLookupTool,
)


class TestRecipientInstructions:
    def test_no_recipients_is_not_an_error_and_points_to_recipient_types(self):
        instructions = RecipientLookupTool._build_instructions(0, is_identifier_match=False)

        assert instructions == NO_RECIPIENT_INSTRUCTIONS
        assert "not an error" in instructions
        assert "list_recipient_types" in instructions

    def test_no_recipients_points_award_identifiers_to_lookup_award(self):
        instructions = RecipientLookupTool._build_instructions(0, is_identifier_match=False)

        assert "lookup_award" in instructions
        assert "Do not repeat the same query" in instructions

    def test_exact_identifier_match_is_used_as_returned(self):
        instructions = RecipientLookupTool._build_instructions(1, is_identifier_match=True)

        assert instructions == IDENTIFIER_MATCH_INSTRUCTIONS

    @pytest.mark.parametrize("recipient_count", [1, 3])
    def test_names_are_candidates_to_choose_from(self, recipient_count):
        # Even a single name is a fuzzy match to confirm, since only an identifier hit is unambiguous
        instructions = RecipientLookupTool._build_instructions(recipient_count, is_identifier_match=False)

        assert instructions == RECIPIENT_NAMES_INSTRUCTIONS
        assert "Do not include every result" in instructions
