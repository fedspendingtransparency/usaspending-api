from typing import get_args

from usaspending_api.llm.models.py_models import RecipientType
from usaspending_api.llm.tools.list_recipient_types import (
    RECIPIENT_TYPES,
    list_recipient_types,
    list_recipient_types_tool,
)


def _all_entries() -> list[dict[str, str]]:
    return [entry for entries in RECIPIENT_TYPES.values() for entry in entries]


class TestRecipientTypesData:
    """Tests for the static RECIPIENT_TYPES catalog"""

    def test_covers_every_recipient_type_value_exactly(self):
        """Every RecipientType Literal value appears once, with no extras."""
        catalog_values = [entry["value"] for entry in _all_entries()]
        literal_values = list(get_args(RecipientType))

        assert sorted(catalog_values) == sorted(literal_values)
        # No duplicates across categories.
        assert len(catalog_values) == len(set(catalog_values))

    def test_every_entry_has_value_and_nonempty_label(self):
        for entry in _all_entries():
            assert set(entry.keys()) == {"value", "label"}
            assert entry["value"]
            assert entry["label"]


class TestListRecipientTypesFunction:
    """Tests for the list_recipient_types function"""

    def test_returns_the_catalog(self):
        assert list_recipient_types() is RECIPIENT_TYPES

    def test_callable_with_no_input(self):
        """The assistant invokes tools as function(**tool_input); this tool takes no input."""
        assert list_recipient_types_tool.function(**{}) is RECIPIENT_TYPES


class TestListRecipientTypesTool:
    """Tests for the AITool wiring"""

    def test_tool_name(self):
        assert list_recipient_types_tool.description.name == "list_recipient_types"

    def test_input_schema_accepts_no_arguments(self):
        schema = list_recipient_types_tool.description.input_schema
        assert schema["properties"] == {}
        assert schema["required"] == []

    def test_logging_ignores_empty_input(self):
        assert list_recipient_types_tool.logging({}) == "Listing recipient types."
