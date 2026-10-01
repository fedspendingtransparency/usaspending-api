from typing import get_args

from usaspending_api.llm.models.py_models import DEFCode
from usaspending_api.llm.tools.list_defc_codes import (
    DEFC_CODES,
    list_defc_codes,
    list_defc_codes_tool,
)


def _all_entries() -> list[dict[str, str]]:
    return [entry for entries in DEFC_CODES.values() for entry in entries]


class TestDefcCodesData:
    """Tests for the static DEFC_CODES catalog"""

    def test_covers_every_defc_code_exactly(self):
        """Every DEFCode Literal value appears once, with no extras."""
        catalog_codes = [entry["code"] for entry in _all_entries()]
        literal_codes = list(get_args(DEFCode))

        assert sorted(catalog_codes) == sorted(literal_codes)
        # No duplicates across groups.
        assert len(catalog_codes) == len(set(catalog_codes))

    def test_every_entry_has_code_and_nonempty_label(self):
        for entry in _all_entries():
            assert set(entry.keys()) == {"code", "label"}
            assert entry["code"]
            assert entry["label"]


class TestListDefcCodesFunction:
    """Tests for the list_defc_codes function"""

    def test_returns_the_catalog(self):
        assert list_defc_codes() is DEFC_CODES

    def test_callable_with_no_input(self):
        """The assistant invokes tools as function(**tool_input); this tool takes no input."""
        assert list_defc_codes_tool.function(**{}) is DEFC_CODES


class TestListDefcCodesTool:
    """Tests for the AITool wiring"""

    def test_tool_name(self):
        assert list_defc_codes_tool.description.name == "list_defc_codes"

    def test_input_schema_accepts_no_arguments(self):
        schema = list_defc_codes_tool.description.input_schema
        assert schema["properties"] == {}
        assert schema["required"] == []

    def test_logging_ignores_empty_input(self):
        assert list_defc_codes_tool.logging({}) == "Listing DEFC codes."
