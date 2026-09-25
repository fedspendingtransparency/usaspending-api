import json

from django.core.management import call_command
from django.test import override_settings

from usaspending_api.llm.evals.assistants import filter_search
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, ToolCall


def write_dataset(tmp_path):
    dataset_path = tmp_path / "config.json"
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
        tool_calls=(ToolCall(name="execute_filter"),),
        output={"selectedAwardIDs": {"N0001917C0001": {}}},
    )


def test_run_llm_eval_command_outputs_json(tmp_path, monkeypatch, caplog):
    import logging

    write_dataset(tmp_path)
    monkeypatch.setattr(filter_search, "run_eval_case", fake_eval_observation)

    # Capture INFO level logs from the management command
    caplog.set_level(logging.INFO, logger="usaspending_api.llm.management.commands.run_llm_eval")

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case_names=["1"],
            fail_under=1.0,
            format="json",
        )

    # Find the JSON output in the captured logs
    json_output = None
    for record in caplog.records:
        if record.levelname == "INFO" and record.message.strip().startswith("{"):
            json_output = record.message
            break

    assert json_output is not None, "No JSON output found in logs"
    result = json.loads(json_output)

    assert result["assistant"] == "filter_search"
    assert result["dataset"] == "config"
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


def test_run_llm_eval_command_logs_warning_when_score_is_below_threshold(tmp_path, monkeypatch, caplog):
    import logging

    write_dataset(tmp_path)

    def failing_observation(_: EvalCase) -> EvalObservation:
        return EvalObservation(
            tool_calls=(ToolCall(name="lookup_location"),),
            output={"timePeriodType": "dr"},
        )

    monkeypatch.setattr(filter_search, "run_eval_case", failing_observation)

    # Capture WARNING level logs from the management command
    caplog.set_level(logging.WARNING, logger="usaspending_api.llm.management.commands.run_llm_eval")

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case_names=["1"],
            fail_under=1.0,
        )

    # Check that a warning was logged
    assert any("below required threshold" in record.message for record in caplog.records)
