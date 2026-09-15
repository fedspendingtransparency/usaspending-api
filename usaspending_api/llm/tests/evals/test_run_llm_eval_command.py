import json

from django.core.management import call_command
from django.test import override_settings

from usaspending_api.llm.evals.assistants import filter_search
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, ToolCall


def write_dataset(tmp_path):
    """Create the dataset consumed by the command test."""
    dataset_path = tmp_path / "filter_search.csv"
    dataset_path.write_text("""
                                id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
                                1,"How much did Clark Construction receive in contracts for FY25?","timePeriodType = fy
                                timePeriodFy = [""2025""]
                                selectedRecipients = [""Clark Construction""]", "lookup_recipient
                                execute_filter","multi_filter
                                temporal","Approved recipient and fiscal-year case",yes,"SME approved"
                                2,"Show me all transactions for award PIID N0001917C0001","selectedAwardIDs = {""N0001917C0001"": {}}","execute_filter","single_filter","",yes,""
                            """,
                            encoding="utf-8",
    )
    return dataset_path


def fake_eval_observation(case: EvalCase) -> EvalObservation:
    """Returns a deterministic observation matching the selected case. Replaces run_eval_case() for command tests."""
    if case.name == "1":
        return EvalObservation(
            tool_calls=(
                ToolCall(
                    name="lookup_recipient",
                    arguments={
                        "query": "Clark Construction",
                    },
                ),
                ToolCall(
                    name="execute_filter",
                    arguments={
                        "timePeriodType": "fy",
                        "timePeriodFY": ["2025"],
                        "selectedRecipients": ["CLARK CONSTRUCTION"],
                    },
                ),
            ),
            output={
                "timePeriodType": "fy",
                "timePeriodFY": ["2025"],
                "selectedRecipients": ["CLARK CONSTRUCTION"],
            },
            metadata={
                "session_id": "test-session-1",
            },
        )

    return EvalObservation(
        tool_calls=(
            ToolCall(
                name="execute_filter",
                arguments={
                    "selectedAwardIDs": {
                        "N0001917C0001": {},
                    },
                },
            ),
        ),
        output={
            "selectedAwardIDs": {
                "N0001917C0001": {},
            },
        },
        metadata={
            "session_id": "test-session-2",
        },
    )


@override_settings()
def test_run_llm_eval_command_outputs_json(tmp_path, monkeypatch, capsys):
    """
    Verify that the command:
    - loads the CSV;
    - resolves the filter_search evaluator;
    - uses the fake observation;
    - produces JSON;
    - returns a passing result.
    """
    write_dataset(tmp_path)

    monkeypatch.setattr(
        filter_search,
        "run_eval_case",
        fake_eval_observation,
    )

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case="1",
            fail_under=1.0,
            format="json",
        )

    output = capsys.readouterr().out
    result = json.loads(output)

    assert result["assistant"] == "filter_search"
    assert result["dataset"] == "filter_search"
    assert result["case_count"] == 1
    assert result["passed_count"] == 1
    assert result["failed_count"] == 0
    assert result["score"] == 1.0
    assert result["passed"] is True


def test_run_llm_eval_command_can_select_multiple_cases(tmp_path, monkeypatch):
    """The --case option should be able to be supplied multiple times (despite IDE warnings)."""
    write_dataset(tmp_path)

    monkeypatch.setattr(
        filter_search,
        "run_eval_case",
        fake_eval_observation,
    )

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            case_names=["1", "2"],
            fail_under=1.0,
        )


def test_run_llm_eval_command_can_filter_by_tag(tmp_path, monkeypatch):
    """The command can filter cases run as those associated with a tag."""
    write_dataset(tmp_path)

    monkeypatch.setattr(
        filter_search,
        "run_eval_case",
        fake_eval_observation,
    )

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        call_command(
            "run_llm_eval",
            assistant="filter_search",
            tag="single_filter",
            fail_under=1.0,
        )


def test_run_llm_eval_command_fails_when_score_is_below_threshold(tmp_path, monkeypatch):
    """
    A failing observation causes the command to raise CommandError when its aggregate
    score does not satisfy --fail-under.
    """
    write_dataset(tmp_path)

    def failing_observation(_: EvalCase) -> EvalObservation:
        return EvalObservation(
            tool_calls=(
                ToolCall(name="lookup_location"),
            ),
            output={
                "timePeriodType": "dr",
            },
        )

    monkeypatch.setattr(
        filter_search,
        "run_eval_case",
        failing_observation,
    )

    with override_settings(LLM_EVAL_DATASET_DIRECTORY=str(tmp_path)):
        from django.core.management.base import CommandError

        try:
            call_command(
                "run_llm_eval",
                assistant="filter_search",
                case="1",
                fail_under=1.0,
            )
        except CommandError as exc:
            assert "below required threshold" in str(exc)
        else:
            raise AssertionError(
                "Expected run_llm_eval to fail below the threshold."
            )