"""
This file loads the ground truth dataset into the application.

It assumes the ground truth document is a UTF-8-formatted CSV file with specific headers.
It is intended to provide flexibility around the location of the ground truth document in case it changes
between environments.

This is the only file within the Eval framework that understands the structure of the ground truth document.
"""

import csv
import json
from pathlib import Path
from typing import Any

from django.conf import settings

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.models import EvalCase, ToolExpectation

# Every ground truth dataset must provide these headers. Requiring all headers keeps the dataset consistent and
# makes it possible to preserve metadata.
REQUIRED_COLUMNS = {
    "id",
    "query",
    "expected_output",
    "expected_tools",
    "tags",
    "notes",
    "approved",
    "sme_validation_notes",
}

# Accepted values in the "approved" column. Generalized as truthy/falsy to provide flexibility.
TRUE_VALUES = {"1", "true", "yes"}
FALSE_VALUES = {"0", "false", "no"}

# Default location of the ground truth dataset.
# TODO: Determine actual location. This is just a placeholder.
DEFAULT_DATASET_DIR = Path(__file__).resolve().parent / "ground-truth"


def dataset_directory() -> Path:
    """
    Return the directory containing the evaluation datasets. Useful if the location needs to be overridden.
    """
    configured_dir = str(getattr(settings, "LLM_EVAL_DATASET_DIR", None))
    return Path(configured_dir) if configured_dir else DEFAULT_DATASET_DIR


def resolve_dataset_path(dataset_name: str) -> Path:
    """
    Resolve a logical dataset name such as "ground-truth" to "ground-truth.csv".
    """
    dataset_path = dataset_directory() / f"{dataset_name}.csv"

    if dataset_path.suffix != ".csv":
        raise DatasetError("Evaluation datasets must use the .csv extension.")
    if not dataset_path.exists():
        raise DatasetError(f"Evaluation dataset does not exist: {dataset_path}.")
    if not dataset_path.is_file():
        raise DatasetError(f"Evaluation dataset is not a file: {dataset_path}.")

    return dataset_path


def parse_approved(value: str, case_id: str) -> bool:
    """
    Parses the CSV 'approved' field into an actual boolean.

    Approved cases are run by default. Unapproved cases remain in the dataset
    for drafting and SME review, but are skipped unless --include-unapproved is explicitly used.
    """
    normalized_value = value.strip().lower()

    if normalized_value in TRUE_VALUES:
        return True

    if normalized_value in FALSE_VALUES:
        return False

    raise DatasetError(f"Case '{case_id}' has invalid approved value '{value}'. Use yes/no, true/false, or 1/0.")


def parse_scalar(value: str) -> Any:
    """
    Convert a CSV output value into a useful Python value when possible.

    Examples:
        - `2025` => 2025
        - `true` => True
        - `null` => None
        - `"value"` => "value"
    """
    normalized_value = value.strip()

    if not normalized_value:
        return ""

    try:
        return json.loads(normalized_value)
    except json.JSONDecodeError:
        return normalized_value


def assign_nested_value(target: dict[str, Any], dotted_key: str, value: Any, case_id: str) -> None:
    """
    Add one dotted CSV key to a nested Python dict.

    Example:
        Input:
            dotted_key = "time_period.fiscal_year"
            value = 2025

        Output:
            {
                "time_period": {
                    "fiscal_year": 2025,
                },
            }

    The intention here is to allow the CSV to remain easy to read/edit while still producing a structured
    object that can be compared to the final output.
    """
    key_path = [key.strip() for key in dotted_key.split(".") if key.strip()]

    if not key_path:
        raise DatasetError(f"Case '{case_id}' contains an empty expected-output key.")

    current = target

    for key in key_path[:-1]:
        existing_value = current.get(key)

        if existing_value is None:
            current[key] = {}
        elif not isinstance(existing_value, dict):
            raise DatasetError(f"Case '{case_id}' has conflicting output paths for '{dotted_key}'.")

        current = current[key]

    current[key_path[-1]] = value


def parse_expected_output(value: str, case_id: str) -> dict[str, Any]:
    """
    Parse newline-separated `key = value` expressions from expected_output column.

    CSV Example:
        recipient = Clark Construction
        time_period.fiscal_year = 2025
        award_type = Contracts

    Parsed result:
        {
            "recipient": "Clark Construction",
            "time_period": {
                "fiscal_year": 2025,
            },
            "award_type": "Contracts",
        }
    """
    expected_output = dict[str, Any] = {}

    for line_number, raw_line in enumerate(value.splitlines(), start=1):
        line = raw_line.strip()

        if not line:
            continue

        if "=" not in line:
            raise DatasetError(
                f"Case '{case_id}' expected_output line {line_number} must use '<key> = <value>' format."
            )

        raw_key, raw_value = line.split("=", maxsplit=1)
        key = raw_key.strip()

        if not key:
            raise DatasetError(f"Case '{case_id}' expected_output line {line_number} has no key.")

        assign_nested_value(
            target=expected_output,
            dotted_key=key,
            value=parse_scalar(raw_value),
            case_id=case_id,
        )

    if not expected_output:
        raise DatasetError(f"Case '{case_id}' must define at least one expected output value.")

    return expected_output


def parse_expected_tools(value: str, case_id: str) -> tuple[ToolExpectation, ...]:
    """
    Parses newline-separated tool names.

    CSV example input:
        recipient
        time_period
        award_type

    Parsed output:
        (
            ToolExpectation(name="recipient"),
            ToolExpectation(name="time_period"),
            ToolExpectation(name="award_type"),
        )

    The tuple preserves source ordering. The initial evaluator treats tool order as significant because tool order
    may indicate how the assistant understood and constructed filters.
    """
    tool_names = [tool_name.strip() for tool_name in value.splitlines() if tool_name.strip()]

    if not tool_names:
        raise DatasetError(f"Case '{case_id}' must define at least one expected tool.")

    return tuple(ToolExpectation(name=tool_name) for tool_name in tool_names)


def parse_tags(value: str) -> list[str]:
    """
    Parses newline-separated tags into a list.

    Tags are metadata rather than correctness criteria. They support targeted command executions, for example:

        python manage.py run_llm_eval --assistant filter_search --tag temporal
    """
    return [tag.strip() for tag in value.splitlines() if tag.strip()]


def parse_csv_cases(
    dataset_path: Path,
    *,
    include_unapproved: bool = False,
    tags: set[str] | None = None,
) -> list[EvalCase]:
    """
    Load a CSV file and return its selected cases.

    By default:
        - Unapproved cases are excluded.
        - All tags are included.
        - Every included row becomes one EvalCase.

    `include_unapproved=True` is intended for manually testing draft cases.
    `tags={"temporal"}` limits execution to cases with that tag.
    """
    cases: list[EvalCase] = []
    seen_case_ids: set[str] = set()

    try:
        with dataset_path.open(encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            missing_columns = REQUIRED_COLUMNS - set(reader.fieldnames or [])

            if missing_columns:
                raise DatasetError(f"Evaluation CSV is missing required columns: {', '.join(sorted(missing_columns))}")

            for row_number, row in enumerate(reader, start=2):
                case_id = (row.get("id") or "").strip()

                if not case_id:
                    raise DatasetError(f"Dataset row {row_number} has no id.")

                if case_id in seen_case_ids:
                    raise DatasetError(f"Evaluation CSV has a duplicate case ID: '{case_id}'")

                seen_case_ids.add(case_id)

                query = row.get("query" or "").strip()

                if not query:
                    raise DatasetError(f"Case '{case_id}' must define a query.")

                approved = parse_approved(row.get("approved") or "", case_id)
                case_tags = parse_tags(row.get("tags") or "")

                # Draft cases remain in source control but do not alter local or CI eval results
                # until they are approved.
                if not approved and not include_unapproved:
                    continue

                # When tags are specified, a case must include at least one.
                if tags and not tags.intersection(case_tags):
                    continue

                cases.append(
                    EvalCase(
                        name=case_id,
                        input={"query": query},
                        expected_tool_calls=parse_expected_tools(
                            row.get("expected_tools") or "",
                            case_id,
                        ),
                        expected_output=parse_expected_output(
                            row.get("expected_output") or "",
                            case_id,
                        ),
                        metadata={
                            "approved": approved,
                            "tags": case_tags,
                            "notes": (row.get("notes") or "").strip(),
                            "sme_validation_notes": (row.get("sme_validation_notes") or "").strip(),
                        },
                    )
                )
    except OSError as exc:
        raise DatasetError(f"Unable to read evaluation dataset '{dataset_path}': {exc}") from exc

    return cases


def load_cases(
    dataset_name: str,
    *,
    selected_case_names: set[str] | None = None,
    include_unapproved: bool = False,
    tags: set[str] | None = None,
) -> list[EvalCase]:
    """
    Load all eligible cases, then optionally narrow them by case ID.

    The command passes case IDs through --case:

        python manage.py run_llm_eval \
            --assistant filter_search \
            --case 1 \
            --case 3
    """
    cases = parse_csv_cases(
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
