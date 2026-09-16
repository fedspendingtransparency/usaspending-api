"""
This file loads the ground truth dataset into the application.

It assumes the ground truth document is a UTF-8-formatted JSON file containing a list of cases.
It is intended to provide flexibility around the location of the ground truth document in case it changes
between environments.

This is the only file within the Eval framework that understands the structure of the ground truth document.
"""

import json
from pathlib import Path
from typing import Any

from django.conf import settings

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.models import EvalCase, ToolExpectation

REQUIRED_FIELDS = {
    "id",
    "query",
    "expected_output",
    "expected_tools",
    "tags",
    "notes",
    "approved",
    "sme_validation_notes",
}

TRUE_VALUES = {"1", "true", "yes"}
FALSE_VALUES = {"0", "false", "no"}

# Default location of the ground truth dataset.
DEFAULT_DATASET_DIR = Path(__file__).resolve().parent / "data"


def dataset_directory() -> Path:
    """Return the configured directory containing evaluation datasets."""
    configured_dir = getattr(settings, "LLM_EVAL_DATASET_DIRECTORY", None) or getattr(
        settings,
        "LLM_EVAL_DATASET_DIR",
        None,
    )
    return Path(configured_dir) if configured_dir else DEFAULT_DATASET_DIR


def resolve_dataset_path(dataset_name: str) -> Path:
    """Resolve a logical dataset name such as "ground_truth" to "ground_truth.json"."""
    dataset_path = dataset_directory() / f"{dataset_name}.json"

    if dataset_path.suffix != ".json":
        raise DatasetError("Evaluation datasets must use the .json extension.")
    if not dataset_path.exists():
        raise DatasetError(f"Evaluation dataset does not exist: {dataset_path}.")
    if not dataset_path.is_file():
        raise DatasetError(f"Evaluation dataset is not a file: {dataset_path}.")

    return dataset_path


def parse_approved(value: Any, case_id: str) -> bool:
    """Parse the JSON ``approved`` field into a boolean."""
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized_value = value.strip().lower()

        if normalized_value in TRUE_VALUES:
            return True

        if normalized_value in FALSE_VALUES:
            return False

    raise DatasetError(f"Case '{case_id}' has invalid approved value '{value}'. Use a JSON boolean.")


def parse_expected_output(value: Any, case_id: str) -> dict[str, Any]:
    """Validate and return the expected output object from a JSON case."""
    if not isinstance(value, dict) or not value:
        raise DatasetError(f"Case '{case_id}' must define a non-empty expected_output object.")

    if not all(isinstance(key, str) and key for key in value):
        raise DatasetError(f"Case '{case_id}' expected_output keys must be non-empty strings.")

    return value


def parse_expected_tools(value: Any, case_id: str) -> tuple[ToolExpectation, ...]:
    """Parse the expected tool definitions from a JSON array."""
    if not isinstance(value, list) or not value:
        raise DatasetError(f"Case '{case_id}' must define at least one expected tool.")

    expected_tools = []
    for tool in value:
        if isinstance(tool, str) and tool.strip():
            expected_tools.append(ToolExpectation(name=tool.strip()))
        elif isinstance(tool, dict) and isinstance(tool.get("name"), str) and tool["name"].strip():
            arguments = tool.get("arguments")
            if arguments is not None and not isinstance(arguments, dict):
                raise DatasetError(f"Case '{case_id}' has invalid arguments for expected tool '{tool['name']}'.")
            expected_tools.append(ToolExpectation(name=tool["name"].strip(), arguments=arguments))
        else:
            raise DatasetError(f"Case '{case_id}' contains an invalid expected tool definition.")

    return tuple(expected_tools)


def parse_tags(value: Any, case_id: str) -> list[str]:
    """Parse the metadata tags from a JSON array."""
    if not isinstance(value, list) or not all(isinstance(tag, str) and tag.strip() for tag in value):
        raise DatasetError(f"Case '{case_id}' tags must be an array of non-empty strings.")

    return [tag.strip() for tag in value]


def _load_dataset(dataset_path: Path) -> list[dict[str, Any]]:
    try:
        with dataset_path.open(encoding="utf-8") as json_file:
            dataset = json.load(json_file)
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetError(f"Unable to read evaluation dataset '{dataset_path}': {exc}") from exc

    if not isinstance(dataset, list):
        raise DatasetError("Evaluation dataset JSON must contain an array of cases.")

    return dataset


def _parse_case_id(value: Any, case_number: int) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise DatasetError(f"Dataset case {case_number} has an invalid id.")

    case_id = str(value).strip()
    if not case_id:
        raise DatasetError(f"Dataset case {case_number} has no id.")

    return case_id


def _parse_query(value: Any, case_id: str) -> str:
    query = value.strip() if isinstance(value, str) else ""
    if not query:
        raise DatasetError(f"Case '{case_id}' must define a query.")

    return query


def _parse_case(row: Any, case_number: int, seen_case_ids: set[str]) -> EvalCase:
    if not isinstance(row, dict):
        raise DatasetError(f"Dataset case {case_number} must be a JSON object.")

    missing_fields = REQUIRED_FIELDS - row.keys()
    if missing_fields:
        raise DatasetError(
            f"Evaluation JSON case {case_number} is missing required fields: {', '.join(sorted(missing_fields))}"
        )

    case_id = _parse_case_id(row["id"], case_number)
    if case_id in seen_case_ids:
        raise DatasetError(f"Evaluation JSON has a duplicate case ID: '{case_id}'")

    seen_case_ids.add(case_id)
    approved = parse_approved(row["approved"], case_id)
    case_tags = parse_tags(row["tags"], case_id)

    return EvalCase(
        name=case_id,
        input={"query": _parse_query(row["query"], case_id)},
        expected_tool_calls=parse_expected_tools(row["expected_tools"], case_id),
        expected_output=parse_expected_output(row["expected_output"], case_id),
        metadata={
            "approved": approved,
            "tags": case_tags,
            "notes": row["notes"].strip() if isinstance(row["notes"], str) else "",
            "sme_validation_notes": (
                row["sme_validation_notes"].strip() if isinstance(row["sme_validation_notes"], str) else ""
            ),
        },
    )


def _case_is_selected(case: EvalCase, include_unapproved: bool, tags: set[str] | None) -> bool:
    return (include_unapproved or case.metadata["approved"]) and (
        not tags or bool(tags.intersection(case.metadata["tags"]))
    )


def parse_json_cases(
    dataset_path: Path,
    *,
    include_unapproved: bool = False,
    tags: set[str] | None = None,
) -> list[EvalCase]:
    """Load a JSON file and return the selected, validated cases."""
    cases = []
    seen_case_ids: set[str] = set()

    for case_number, row in enumerate(_load_dataset(dataset_path), start=1):
        case = _parse_case(row, case_number, seen_case_ids)
        if _case_is_selected(case, include_unapproved, tags):
            cases.append(case)

    return cases


def load_cases(
    dataset_name: str,
    *,
    selected_case_names: set[str] | None = None,
    include_unapproved: bool = False,
    tags: set[str] | None = None,
) -> list[EvalCase]:
    """Load all eligible cases, then optionally narrow them by case ID."""
    cases = parse_json_cases(
        resolve_dataset_path(dataset_name),
        include_unapproved=include_unapproved,
        tags=tags,
    )

    if not selected_case_names:
        return cases

    selected_cases = [case for case in cases if case.name in selected_case_names]
    found_names = {case.name for case in selected_cases}
    missing_names = selected_case_names - found_names

    if missing_names:
        formatted_names = ", ".join(sorted(missing_names))
        raise DatasetError(f"Requested case(s) are unavailable in dataset '{dataset_name}': {formatted_names}")

    return selected_cases
