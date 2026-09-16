import json
from typing import Any

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.ingestion.models import FilterMapping, WorkbookData

TRUE_VALUES = {"1", "true", "yes", "y"}
FALSE_VALUES = {"0", "false", "no", "n"}


def transform_workbook(workbook: WorkbookData) -> list[dict[str, Any]]:
    """Convert validated workbook rows into the runtime JSON schema."""
    mappings = _mapping_index(workbook.filter_mappings)
    cases = []
    seen_ids: set[str] = set()

    for row in workbook.ground_truth_rows:
        case_id = _case_id(row.get("id"))
        if case_id in seen_ids:
            raise DatasetError(f"Ground Truth contains duplicate case id '{case_id}'.")
        seen_ids.add(case_id)

        cases.append(
            {
                "id": row["id"],
                "query": _required_text(row.get("query"), case_id, "query"),
                "expected_output": _transform_output(row.get("expected_output"), mappings, case_id),
                "expected_tools": _transform_tools(row.get("expected_tools"), mappings, case_id),
                "tags": _parse_json_or_list(row.get("tags"), case_id, "tags"),
                "notes": str(row.get("notes") or "").strip(),
                "approved": _parse_bool(row.get("approved"), case_id),
                "sme_validation_notes": str(row.get("sme_validation_notes") or "").strip(),
            }
        )

    return cases


def _mapping_index(mappings: tuple[FilterMapping, ...]) -> dict[tuple[str, str], FilterMapping]:
    return {
        (mapping.mapping_type, mapping.source_name): mapping for mapping in mappings
    }


def _transform_output(value: Any, mappings: dict[tuple[str, str], FilterMapping], case_id: str) -> dict[str, Any]:
    parsed = _parse_json_object(value, case_id, "expected_output")
    transformed: dict[str, Any] = {}
    fiscal_year_present = False

    for source_name, source_value in _iter_filter_values(parsed, mappings):
        mapping = mappings.get(("filter", source_name))
        if mapping is None:
            raise DatasetError(
                f"Case '{case_id}' uses unmapped filter field '{source_name}'."
            )

        target_name = mapping.target_name
        if target_name in transformed:
            raise DatasetError(
                f"Case '{case_id}' maps multiple fields to '{target_name}'."
            )

        transformed[target_name] = _apply_value_transform(
            source_value,
            mapping,
            case_id,
        )
        fiscal_year_present = fiscal_year_present or _value_transform(mapping.target_name) == "fiscal_year"

    if fiscal_year_present:
        transformed.setdefault("timePeriodType", "fy")

    return transformed


def _transform_tools(value: Any, mappings: dict[tuple[str, str], FilterMapping], case_id: str) -> list[Any]:
    parsed = _parse_json_array(value, case_id, "expected_tools")
    transformed = []

    for tool in parsed:
        source_name = tool["name"] if isinstance(tool, dict) else tool
        if not isinstance(source_name, str) or not source_name.strip():
            raise DatasetError(f"Case '{case_id}' contains an invalid expected tool.")

        mapping = mappings.get(("tool", source_name.strip()))
        if mapping is None:
            raise DatasetError(
                f"Case '{case_id}' uses unmapped tool '{source_name}'."
            )

        if isinstance(tool, dict):
            transformed.append({**tool, "name": mapping.target_name})
        else:
            transformed.append(mapping.target_name)

    return transformed


def _apply_value_transform(value: Any, mapping: FilterMapping, case_id: str) -> Any:
    transform = _value_transform(mapping.target_name)
    transformed = value

    if transform == "scalar_to_list":
        transformed = value if isinstance(value, list) else [value]
    elif transform == "fiscal_year":
        values = value if isinstance(value, list) else [value]
        transformed = [str(item) for item in values]
    elif transform == "structured" and not isinstance(value, dict):
        raise DatasetError(
            f"Case '{case_id}' field '{mapping.source_name}' must be an object."
        )

    return transformed


def _value_transform(target_name: str) -> str:
    transforms = {
        "timePeriodFY": "fiscal_year",
        "selectedLocations": "structured",
        "selectedAwardIDs": "structured",
        "selectedRecipients": "scalar_to_list",
        "selectedRecipient": "scalar_to_list",
        "awardType": "scalar_to_list",
    }
    return transforms.get(target_name, "identity")


def _iter_filter_values(
    value: dict[str, Any],
    mappings: dict[tuple[str, str], FilterMapping],
    prefix: str = "",
) -> list[tuple[str, Any]]:
    """Flatten simple dotted fields while preserving mapped structured values."""
    flattened = []

    for key, child in value.items():
        path = f"{prefix}.{key}" if prefix else key
        if ("filter", path) in mappings or not isinstance(child, dict):
            flattened.append((path, child))
        else:
            flattened.extend(_iter_filter_values(child, mappings, path))

    return flattened


def _case_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise DatasetError("Ground Truth contains a case with an invalid id.")
    case_id = str(value).strip()
    if not case_id:
        raise DatasetError("Ground Truth contains a case with an empty id.")
    return case_id


def _required_text(value: Any, case_id: str, field_name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DatasetError(f"Case '{case_id}' must define {field_name}.")
    return result


def _parse_json_object(value: Any, case_id: str, field_name: str) -> dict[str, Any]:
    parsed = _parse_json(value, case_id, field_name)
    if not isinstance(parsed, dict) or not parsed:
        raise DatasetError(f"Case '{case_id}' {field_name} must be a non-empty object.")
    return parsed


def _parse_json_array(value: Any, case_id: str, field_name: str) -> list[Any]:
    parsed = _parse_json(value, case_id, field_name)
    if not isinstance(parsed, list) or not parsed:
        raise DatasetError(f"Case '{case_id}' {field_name} must be a non-empty array.")
    return parsed


def _parse_json_or_list(value: Any, case_id: str, field_name: str) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None or (isinstance(value, str) and not value.strip()):
        return []
    if not isinstance(value, str):
        raise DatasetError(f"Case '{case_id}' {field_name} must contain values.")

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = [item.strip() for item in value.replace("\n", ",").split(",") if item.strip()]

    if not isinstance(parsed, list) or not all(isinstance(item, str) and item.strip() for item in parsed):
        raise DatasetError(f"Case '{case_id}' {field_name} must be a list of non-empty strings.")
    return [item.strip() for item in parsed]


def _parse_json(value: Any, case_id: str, field_name: str) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        raise DatasetError(f"Case '{case_id}' {field_name} must contain JSON text.")

    try:
        return json.loads(value)
    except json.JSONDecodeError as error:
        raise DatasetError(f"Case '{case_id}' {field_name} contains invalid JSON: {error}") from error


def _parse_bool(value: Any, case_id: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise DatasetError(f"Case '{case_id}' has invalid approved value '{value}'.")
