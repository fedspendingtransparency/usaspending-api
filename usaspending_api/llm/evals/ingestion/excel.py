import io
from typing import Any

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.ingestion.models import (
    FilterMapping,
    GroundTruthSource,
    WorkbookData,
)

REQUIRED_SHEETS = {"Ground Truth", "Filter Dictionary"}
REQUIRED_GROUND_TRUTH_COLUMNS = {
    "id",
    "query",
    "expected_output",
    "expected_tools",
    "tags",
    "notes",
    "approved",
    "sme_validation_notes",
}
REQUIRED_DICTIONARY_COLUMNS = {
    "Filter",
    "Subfilter",
    "Naming Convention",
    "Backend Naming Convention",
}
RUNTIME_TOOL_NAMES = {
    "lookup_codes",
    "lookup_location",
    "lookup_recipient",
    "execute_filter",
}


def read_workbook(source: GroundTruthSource) -> WorkbookData:
    """Read and validate the required workbook tabs."""
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(
            io.BytesIO(source.content),
            read_only=True,
            data_only=True,
        )
    except ImportError as error:
        raise DatasetError("Excel ingestion requires openpyxl.") from error
    except Exception as exc:
        raise DatasetError(f"Unable to read ground-truth workbook '{source.source_name}': {exc}") from exc

    missing_sheets = REQUIRED_SHEETS - set(workbook.sheetnames)
    if missing_sheets:
        raise DatasetError("Ground-truth workbook is missing worksheet(s): " + ", ".join(sorted(missing_sheets)))

    ground_truth_rows = _read_rows(workbook["Ground Truth"], REQUIRED_GROUND_TRUTH_COLUMNS, "Ground Truth")
    dictionary_rows = _read_rows(workbook["Filter Dictionary"], REQUIRED_DICTIONARY_COLUMNS, "Filter Dictionary")

    return WorkbookData(
        ground_truth_rows=tuple(ground_truth_rows),
        filter_mappings=tuple(_parse_mappings(dictionary_rows)),
        source=source,
    )


def _read_rows(worksheet: Any, required_columns: set[str], sheet_name: str) -> list[dict[str, Any]]:
    """
    Reads and validates rows from an Excel worksheet.
    Returns a list of objects, where each object represents a row (i.e., JSON structure).
    """
    rows = list(worksheet.values)

    if not rows:
        raise DatasetError(f"Worksheet '{sheet_name}' is empty.")

    # Check headers.
    headers = [str(value).strip() if value is not None else "" for value in rows[0]]
    if len(headers) != len(set(headers)):
        raise DatasetError(f"Worksheet '{sheet_name}' contains duplicate headers.")

    missing_columns = required_columns - set(headers)
    if missing_columns:
        raise DatasetError(f"Worksheet '{sheet_name}' is missing column(s): " + ", ".join(sorted(missing_columns)))

    result = []
    for values in rows[1:]:
        # Skip empty rows.
        if all(value is None or str(value).strip() == "" for value in values):
            continue
        # Collect non-empty rows.
        result.append({header: values[index] if index < len(values) else None for index, header in enumerate(headers)})

    return result


def _parse_mappings(rows: list[dict[str, Any]]) -> list[FilterMapping]:
    mappings = []
    seen_keys: set[tuple[str, str]] = set()

    # Reads the "Filter Dictionary" worksheet, and populates a list of mappings.
    for row in rows:
        # The name of the Filter in the worksheet. Not the same as the name of the Pydantic Filter model.
        filter_name = _required_cell(row, "Filter")
        # Subfilter is mostly unused, but a few filters have one.
        subfilter = str(row.get("Subfilter") or "").strip()
        # source_name is what's used in the "Ground Truth" worksheet.
        source_name = _required_cell(row, "Naming Convention")
        # target_name is what's used by the Pydantic model Filters on the backend.
        target_name = _required_cell(row, "Backend Naming Convention")
        # Returns either "tool" or "filter" (will almost always return "filter").
        mapping_type = _mapping_type(filter_name, target_name)
        key = (mapping_type, source_name)

        if key in seen_keys:
            raise DatasetError(f"Filter Dictionary contains a duplicate Naming Convention '{source_name}'.")
        seen_keys.add(key)

        mappings.append(
            FilterMapping(
                mapping_type=mapping_type,
                filter_name=filter_name,
                subfilter=subfilter,
                source_name=source_name,
                target_name=target_name,
            )
        )

    return mappings


def _mapping_type(filter_name: str, target_name: str) -> str:
    """Returns either "tool" or "filter" based on the filter name and the target name."""
    if target_name in RUNTIME_TOOL_NAMES or filter_name.casefold() in {"tool", "tools"}:
        return "tool"
    return "filter"


def _required_cell(row: dict[str, Any], field_name: str) -> str:
    value = str(row.get(field_name) or "").strip()
    if not value:
        raise DatasetError(f"Filter Dictionary rows require {field_name}.")
    return value
