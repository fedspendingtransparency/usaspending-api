from usaspending_api.llm.evals.matchers import MappingSubsetMatcher, ToolCallMatcher
from usaspending_api.llm.evals.models import ToolCall, ToolExpectation


def test_tool_call_matcher_matches_actual_production_tool_names():
    """The matcher compares names persisted in ToolUse.name."""
    result = ToolCallMatcher().compare(
        expected=(
            ToolExpectation(name="lookup_recipient"),
            ToolExpectation(name="execute_filter"),
        ),
        actual=(
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
                },
            ),
        ),
    )

    assert result.passed is True
    assert result.score == 1.0


def test_tool_call_matcher_requires_matching_call_count():
    """
    An extra or missing tool call is a failure under the initial strict policy.
    May be need updating in future iterations if requirements change.
    """
    result = ToolCallMatcher().compare(
        expected=(
            ToolExpectation(name="lookup_recipient"),
            ToolExpectation(name="execute_filter"),
        ),
        actual=(ToolCall(name="lookup_recipient"),),
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "Expected 2 tool call(s)" in result.message


def test_tool_call_matcher_rejects_different_tool_order():
    """Tool order is considered important context for LLM evaluations."""
    result = ToolCallMatcher().compare(
        expected=(
            ToolExpectation(name="lookup_recipient"),
            ToolExpectation(name="execute_filter"),
        ),
        actual=(
            ToolCall(name="execute_filter"),
            ToolCall(name="lookup_recipient"),
        ),
    )

    assert result.passed is False
    assert result.score == 0.0


def test_tool_call_matcher_ignores_arguments_when_not_in_ground_truth():
    """
    The current JSON ground truth supplies tool names but not argument expectations.
    Actual arguments may therefore be present without causing a failure.
    """
    result = ToolCallMatcher().compare(
        expected=(ToolExpectation(name="execute_filter")),
        actual=(
            ToolCall(
                name="execute_filter",
                arguments={
                    "timePeriodType": "fy",
                    "timePeriodFY": ["2025"],
                },
            ),
        ),
    )

    assert result.passed is True


def test_tool_call_matcher_checks_arguments_when_expected():
    """Argument validation becomes active when ToolExpectation.arguments is explicitly populated."""
    result = ToolCallMatcher().compare(
        expected=(
            ToolExpectation(
                name="lookup_recipient",
                arguments={
                    "query": "Clark Construction",
                },
            ),
        ),
        actual=(
            ToolCall(
                name="lookup_recipient",
                arguments={
                    "query": "Different Recipient",
                },
            ),
        ),
    )

    assert result.passed is False
    assert result.score == 0.0


def test_tool_call_matcher_allows_extra_arguments_when_requested():
    result = ToolCallMatcher(allow_extra_actual_arguments=True).compare(
        expected=(
            ToolExpectation(
                name="lookup_recipient",
                arguments={"query": "Clark Construction"},
            ),
        ),
        actual=(
            ToolCall(
                name="lookup_recipient",
                arguments={"query": "Clark Construction", "top_k": 10},
            ),
        ),
    )

    assert result.passed is True
    assert result.score == 1.0


def test_mapping_subset_matcher_supports_nested_output():
    """Expected filter fields may be nested and actual output may contain extra fields not listed in ground truth."""
    result = MappingSubsetMatcher().compare(
        expected={
            "time_period": {
                "start_date": "2024-10-01",
                "end_date": "2025-09-30",
            },
        },
        actual={
            "time_period": {
                "start_date": "2024-10-01",
                "end_date": "2025-09-30",
                "date_type": "custom",
            },
            "metadata": {
                "request_id": "request-123",
            },
        },
    )

    assert result.passed is True
    assert result.score == 1.0


def test_mapping_subset_matcher_rejects_missing_nested_output():
    """A required nested expected value must exist in the actual output."""
    result = MappingSubsetMatcher().compare(
        expected={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
        },
        actual={
            "timePeriodType": "fy",
        },
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "timePeriodFY (missing)" in result.message


def test_mapping_subset_matcher_rejects_wrong_nested_value():
    """A wrong value is a failure even if the containing object exists."""
    result = MappingSubsetMatcher().compare(
        expected={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
        },
        actual={
            "timePeriodType": "fy",
            "timePeriodFY": ["2024"],
        },
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "timePeriodFY" in result.message
