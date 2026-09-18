import json

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import override_settings

from usaspending_api.llm.evals.assistants import filter_search
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, ToolCall


def write_dataset(tmp_path):
    dataset_path = tmp_path / "ground_truth.json"
    dataset_path.write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "query": "How much did Clark Construction receive in contracts for FY25?",
                    "expected_output": {
                        "timePeriodType": "fy",
                        "timePeriodFY": ["2025"],
                        "selectedRecipients": ["CLARK CONSTRUCTION"],
                    },
                    "expected_tools": ["lookup_recipient", "execute_filter"],
                    "tags": ["multi_filter", "temporal"],
                    "notes": "",
                    "approved": True,
                    "sme_validation_notes": "",
                },
                {
                    "id": 2,
                    "query": "Show me all transactions for award PIID N0001917C0001",
                    "expected_output": {
                        "selectedAwardIDs": {"N0001917C0001": {}},
                    },
                    "expected_tools": ["execute_filter"],
                    "tags": ["single_filter"],
                    "notes": "",
                    "approved": True,
                    "sme_validation_notes": "",
                },
            ]
        ),
        encoding="utf-8",
    )


def fake_eval_observation(case: EvalCase) -> EvalObservation:
    if case.name == "1":
        return EvalObservation(
            tool_calls=(
                ToolCall(name="lookup_recipient"),
                ToolCall(name="execute_filter"),
            ),
            output={
                "timePeriodType": "fy",
                "timePeriodFY": ["2025"],
                "selectedRecipients": ["CLARK CONSTRUCTION"],
            },
        )

    return EvalObservation(
        tool_calls=(
            ToolCall(name="execute_filter"),
        ),
        output={"selectedAwardIDs": {"N0001917C0001": {}}},
    )


def test_run_llm_eval_command_outputs_json(tmp_path, monkeypatch, capsys):
    write_dataset(tmp_path)
    monkeypatch.setattr(filter_search, "run_eval_case", fake_eval_observation)

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case_names=["1"],
            fail_under=1.0,
            format="json",
        )

    result = json.loads(capsys.readouterr().out)

    assert result["assistant"] == "filter_search"
    assert result["dataset"] == "ground_truth"
    assert result["case_count"] == 1
    assert result["passed_count"] == 1
    assert result["failed_count"] == 0
    assert result["score"] == 1.0
    assert result["passed"] is True


def test_run_llm_eval_command_can_select_multiple_cases(tmp_path, monkeypatch):
    write_dataset(tmp_path)
    monkeypatch.setattr(filter_search, "run_eval_case", fake_eval_observation)

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case_names=["1", "2"],
            fail_under=1.0,
        )


def test_run_llm_eval_command_can_filter_by_tag(tmp_path, monkeypatch):
    write_dataset(tmp_path)
    monkeypatch.setattr(filter_search, "run_eval_case", fake_eval_observation)

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            tags=["single_filter"],
            fail_under=1.0,
        )


def test_run_llm_eval_command_fails_when_score_is_below_threshold(tmp_path, monkeypatch):
    write_dataset(tmp_path)

    def failing_observation(_: EvalCase) -> EvalObservation:
        return EvalObservation(
            tool_calls=(ToolCall(name="lookup_location"),),
            output={"timePeriodType": "dr"},
        )

    monkeypatch.setattr(filter_search, "run_eval_case", failing_observation)

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        with pytest.raises(CommandError, match="below required threshold"):
            call_command(
                "run_llm_eval",
                assistant="filter_search",
                case_names=["1"],
                fail_under=1.0,
            )
