import json
from pathlib import Path

import pytest

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.loader import parse_json_cases

SAMPLE_CASE = {
    "id": 1,
    "query": "How much did Clark Construction receive in contracts for FY25?",
    "expected_output": {
        "timePeriodType": "fy",
        "timePeriodFY": ["2025"],
        "selectedRecipients": ["CLARK CONSTRUCTION"],
    },
    "expected_tools": ["lookup_recipient", "execute_filter"],
    "tags": ["multi_filter", "temporal"],
    "notes": "Approved recipient and fiscal-year case",
    "approved": True,
    "sme_validation_notes": "SME approved",
}


def write_dataset(tmp_path: Path, cases: list[dict]) -> Path:
    dataset_path = tmp_path / "ground_truth.json"
    dataset_path.write_text(json.dumps(cases), encoding="utf-8")
    return dataset_path


def test_parse_json_cases_builds_expected_case(tmp_path: Path):
    cases = parse_json_cases(write_dataset(tmp_path, [SAMPLE_CASE]))

    assert len(cases) == 1

    case = cases[0]

    assert case.name == "1"
    assert case.input == {"query": SAMPLE_CASE["query"]}
    assert [tool.name for tool in case.expected_tool_calls] == [
        "lookup_recipient",
        "execute_filter",
    ]
    assert case.expected_output == SAMPLE_CASE["expected_output"]
    assert case.metadata == {
        "approved": True,
        "tags": ["multi_filter", "temporal"],
        "notes": "Approved recipient and fiscal-year case",
        "sme_validation_notes": "SME approved",
    }


def test_parse_json_cases_supports_tool_argument_expectations(tmp_path: Path):
    case = {
        **SAMPLE_CASE,
        "expected_tools": [
            {
                "name": "lookup_recipient",
                "arguments": {"query": "Clark Construction"},
            },
            "execute_filter",
        ],
    }

    cases = parse_json_cases(write_dataset(tmp_path, [case]))

    assert cases[0].expected_tool_calls[0].name == "lookup_recipient"
    assert cases[0].expected_tool_calls[0].arguments == {
        "query": "Clark Construction",
    }
    assert cases[0].expected_tool_calls[1].arguments is None


def test_parse_json_cases_excludes_unapproved_cases_by_default(tmp_path: Path):
    draft_case = {
        **SAMPLE_CASE,
        "id": 2,
        "approved": False,
    }

    cases = parse_json_cases(write_dataset(tmp_path, [SAMPLE_CASE, draft_case]))

    assert [case.name for case in cases] == ["1"]


def test_parse_json_cases_can_include_unapproved_cases(tmp_path: Path):
    draft_case = {
        **SAMPLE_CASE,
        "id": 2,
        "approved": False,
    }

    cases = parse_json_cases(
        write_dataset(tmp_path, [SAMPLE_CASE, draft_case]),
        include_unapproved=True,
    )

    assert [case.name for case in cases] == ["1", "2"]


def test_parse_json_cases_filters_by_tag(tmp_path: Path):
    award_case = {
        **SAMPLE_CASE,
        "id": 2,
        "tags": ["award_id"],
        "expected_output": {
            "selectedAwardIDs": {"N0001917C0001": {}},
        },
        "expected_tools": ["execute_filter"],
    }

    cases = parse_json_cases(
        write_dataset(tmp_path, [SAMPLE_CASE, award_case]),
        tags={"award_id"},
    )

    assert [case.name for case in cases] == ["2"]


@pytest.mark.parametrize(
    "field,value,error_message",
    [
        ("approved", "maybe", "invalid approved value"),
        ("expected_output", [], "non-empty expected_output object"),
        ("expected_tools", [], "at least one expected tool"),
        ("tags", ["", "temporal"], "array of non-empty strings"),
    ],
)
def test_parse_json_cases_rejects_invalid_case_fields(tmp_path: Path, field, value, error_message):
    invalid_case = {
        **SAMPLE_CASE,
        field: value,
    }

    with pytest.raises(DatasetError, match=error_message):
        parse_json_cases(write_dataset(tmp_path, [invalid_case]))


def test_parse_json_cases_rejects_missing_required_field(tmp_path: Path):
    invalid_case = {
        key: value
        for key, value in SAMPLE_CASE.items()
        if key != "sme_validation_notes"
    }

    with pytest.raises(DatasetError, match="missing required fields: sme_validation_notes"):
        parse_json_cases(write_dataset(tmp_path, [invalid_case]))


def test_parse_json_cases_rejects_duplicate_case_ids(tmp_path: Path):
    duplicate_case = {
        **SAMPLE_CASE,
        "query": "A different query",
    }

    with pytest.raises(DatasetError, match="duplicate case ID: '1'"):
        parse_json_cases(write_dataset(tmp_path, [SAMPLE_CASE, duplicate_case]))


def test_parse_json_cases_rejects_boolean_case_ids(tmp_path: Path):
    invalid_case = {
        **SAMPLE_CASE,
        "id": True,
    }

    with pytest.raises(DatasetError, match="has an invalid id"):
        parse_json_cases(write_dataset(tmp_path, [invalid_case]))
