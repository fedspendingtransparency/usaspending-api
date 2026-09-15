from unittest.mock import Mock

import pytest

from usaspending_api.llm.evals.assistants import filter_search
from usaspending_api.llm.evals.assistants.filter_search import (
    FilterSearchEval,
    get_final_filter_output,
    get_tool_calls,
    run_eval_case,
)
from usaspending_api.llm.evals.exceptions import ExecutionError
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, ToolCall, ToolExpectation


def make_case(
    *,
    name: str = "1",
    expected_tools: tuple[ToolExpectation, ...] = (
        ToolExpectation(name="lookup_recipient"),
        ToolExpectation(name="execute_filter"),
    ),
    expected_output: dict | None = None,
) -> EvalCase:
    """Build a representative case using the actual Filter schema names."""
    return EvalCase(
        name=name,
        input={
            "query": "How much did Clark Construction receive in contracts for FY25?",
        },
        expected_tool_calls=expected_tools,
        expected_output=expected_output
        or {
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "selectedRecipients": ["CLARK CONSTRUCTION"],
        },
    )


def test_filter_search_eval_passes_when_tools_and_filters_match():
    """A matching tool sequence and matching final filter payload produce a full score of 1.0."""
    case = make_case()

    evaluator = FilterSearchEval.__new__(FilterSearchEval)
    evaluator.tool_call_matcher = filter_search.ToolCallMatcher()
    evaluator.output_matcher = filter_search.MappingSubsetMatcher()

    observation = EvalObservation(
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
    )

    result = evaluator.evaluate(case, observation)

    assert result.passed is True
    assert result.score == 1.0
    assert result.tool_call_match.passed is True
    assert result.output_match.passed is True


def test_filter_search_eval_returns_partial_score_when_filters_are_wrong():
    """Correct tool usage with incorrect final filters receives a score of 0.5."""
    case = make_case()

    evaluator = FilterSearchEval.__new__(FilterSearchEval)
    evaluator.tool_call_matcher = filter_search.ToolCallMatcher()
    evaluator.output_matcher = filter_search.MappingSubsetMatcher()

    observation = EvalObservation(
        tool_calls=(
            ToolCall(name="lookup_recipient"),
            ToolCall(name="execute_filter"),
        ),
        output={
            "timePeriodType": "fy",
            "timePeriodFY": ["2024"],
            "selectedRecipients": ["CLARK CONSTRUCTION"],
        },
    )

    result = evaluator.evaluate(case, observation)

    assert result.passed is False
    assert result.score == 0.5
    assert result.tool_call_match.passed is True
    assert result.output_match.passed is False


def test_get_tool_calls_reads_tool_use_records_in_execution_order(monkeypatch):
    """Tool calls are extracted from the same ToolUse rows written by FilterSearchAssistant.handle_tool_use()."""
    first_tool_use = Mock()
    first_tool_use.name = "lookup_recipient"
    first_tool_use.tool_input = {"query": "Clark Construction"}

    second_tool_use = Mock()
    second_tool_use.name = ("execute_filter",)
    second_tool_use.tool_input = ({"timePeriodType": "fy", "timePeriodFY": ["2025"]},)

    queryset = Mock()
    queryset.order_by.return_value = [
        first_tool_use,
        second_tool_use,
    ]

    manager = Mock()
    manager.filter.return_value = queryset

    monkeypatch.setattr(
        filter_search.ToolUse,
        "objects",
        manager,
    )

    session = Mock()
    calls = get_tool_calls(session)

    assert calls == (
        ToolCall(
            name="lookup_recipient",
            arguments={"query": "Clark Construction"},
        ),
        ToolCall(
            name="execute_filter",
            arguments={
                "timePeriodType": "fy",
                "timePeriodFY": ["2025"],
            },
        ),
    )

    manager.filter.assert_called_once_with(
        message__session=session,
    )
    queryset.order_by.assert_called_once_with(
        "message__order",
        "created_at",
        "id",
    )


def test_get_final_filter_output_uses_successful_execute_filter_call(monkeypatch):
    """
    The adapter uses execute_filter's ToolUse.tool_input, not the returned hash, as the observable final filter payload.
    """
    failed_use = Mock(
        name="failed_use",
        tool_input={"timePeriodType": "invalid"},
        result={"error": "invalid filters"},
    )
    successful_use = Mock(
        name="successful_use",
        tool_input={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "selectedRecipients": ["CLARK CONSTRUCTION"],
        },
        result={"hash": "abc123"},
    )

    queryset = Mock()
    queryset.order_by.return_value = [
        failed_use,
        successful_use,
    ]

    manager = Mock()
    manager.filter.return_value = queryset

    monkeypatch.setattr(
        filter_search.ToolUse,
        "objects",
        manager,
    )
    monkeypatch.setattr(
        filter_search,
        "build_filter_request",
        lambda value: {
            "filters": value,
            "version": "2020-06-01",
        },
    )

    output = get_final_filter_output(Mock())

    assert output == {
        "timePeriodType": "fy",
        "timePeriodFY": ["2025"],
        "selectedRecipients": ["CLARK CONSTRUCTION"],
    }


def test_get_final_filter_output_rejects_missing_successful_completion(monkeypatch):
    """An execution without a successful execute_filter call cannot produce a valid final filter observation."""
    failed_use = Mock(
        name="failed_use",
        tool_input={"timePeriodType": "invalid"},
        result={"error": "invalid filters"},
    )

    queryset = Mock()
    queryset.order_by.return_value = [failed_use]

    manager = Mock()
    manager.filter.return_value = queryset

    monkeypatch.setattr(
        filter_search.ToolUse,
        "objects",
        manager,
    )

    with pytest.raises(ExecutionError, match="without a successful execute_filter call"):
        get_final_filter_output(Mock())


def test_run_eval_case_executes_assistant_and_returns_observation(monkeypatch):
    """
    The concrete adapter:
    - obtains the active assistant;
    - creates a session;
    - invokes assistant.search();
    - reads ToolUse records;
    - reads final filter output;
    - returns an EvalObservation.
    """
    assistant_config = Mock()
    assistant_config.id = 42
    assistant_config.ai_model.model_id = "test-model"

    session = Mock()
    session.id = 123

    assistant_instance = Mock()
    assistant_instance.search.return_value = [
        {
            "search_id": "123",
            "type": "search_start",
            "message": "Thinking...",
        },
        {
            "search_id": "123",
            "type": "search_complete",
            "result": "abc123",
        },
    ]

    monkeypatch.setattr(
        filter_search,
        "get_active_filter_search_assistant",
        lambda: assistant_config,
    )
    monkeypatch.setattr(
        filter_search,
        "create_eval_session",
        lambda value: session,
    )
    monkeypatch.setattr(
        filter_search,
        "FilterSearchAssistant",
        Mock(return_value=assistant_instance),
    )
    monkeypatch.setattr(
        filter_search,
        "get_tool_calls",
        lambda value: (
            ToolCall(
                name="lookup_recipient",
                arguments={"query": "Clark Construction"},
            ),
            ToolCall(
                name="execute_filter",
                arguments={
                    "timePeriodType": "fy",
                    "timePeriodFY": ["2025"],
                },
            ),
        ),
    )
    monkeypatch.setattr(
        filter_search,
        "get_final_filter_output",
        lambda value: {
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
        },
    )

    case = EvalCase(
        name="1",
        input={
            "query": "How much did Clark Construction receive in contracts for FY25?",
        },
        expected_tool_calls=(
            ToolExpectation(name="lookup_recipient"),
            ToolExpectation(name="execute_filter"),
        ),
        expected_output={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
        },
    )

    observation = run_eval_case(case)

    assistant_instance.search.assert_called_once_with("How much did Clark Construction receive in contracts for FY25?")
    session.save.assert_called_once_with(update_fields=["ended_at"])
    assert observation.output == {
        "timePeriodType": "fy",
        "timePeriodFY": ["2025"],
    }
    assert [call.name for call in observation.tool_calls] == [
        "lookup_recipient",
        "execute_filter",
    ]
    assert observation.metadata == {
        "session_id": "123",
        "assistant_id": 42,
        "model_id": "test-model",
        "tool_use_count": 2,
    }


def test_run_eval_case_rejects_search_error(monkeypatch):
    """An endpoint-equivalent search_error event prevents a false passing eval."""
    assistant_config = Mock()
    session = Mock()
    assistant_instance = Mock()
    assistant_instance.search.return_value = [
        {
            "type": "search_error",
            "message": "Bedrock failed",
        },
    ]

    monkeypatch.setattr(
        filter_search,
        "get_active_filter_search_assistant",
        lambda: assistant_config,
    )
    monkeypatch.setattr(
        filter_search,
        "create_eval_session",
        lambda value: session,
    )
    monkeypatch.setattr(
        filter_search,
        "FilterSearchAssistant",
        Mock(return_value=assistant_instance),
    )

    case = make_case()

    with pytest.raises(ExecutionError, match="Bedrock failed"):
        run_eval_case(case)

    session.save.assert_called_once_with(update_fields=["ended_at"])
