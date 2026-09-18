from usaspending_api.llm.evals.matchers import MappingSubsetMatcher, ToolCallMatcher
from usaspending_api.llm.evals.models import ToolCall


def test_tool_call_matcher_matches_actual_production_tool_names():
    result = ToolCallMatcher().compare(
        expected=("lookup_recipient", "execute_filter"),
        actual=(
            ToolCall(name="lookup_recipient"),
            ToolCall(name="execute_filter"),
        ),
    )

    assert result.passed is True
    assert result.score == 1.0


def test_tool_call_matcher_requires_matching_call_count():
    result = ToolCallMatcher().compare(
        expected=("lookup_recipient", "execute_filter"),
        actual=(ToolCall(name="lookup_recipient"),),
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "Expected 2 tool call(s)" in result.message


def test_tool_call_matcher_rejects_different_tool_order():
    result = ToolCallMatcher().compare(
        expected=("lookup_recipient", "execute_filter"),
        actual=(
            ToolCall(name="execute_filter"),
            ToolCall(name="lookup_recipient"),
        ),
    )

    assert result.passed is False
    assert result.score == 0.0


def test_mapping_subset_matcher_supports_nested_output():
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
