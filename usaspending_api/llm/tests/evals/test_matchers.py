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
    # Score: 1 matched tool + 0 position bonus (execute_filter not called) / 3 total points
    assert result.score == 1 / 3
    assert "Missing tools: ['execute_filter']" in result.message
    assert "execute_filter not called" in result.message


def test_tool_call_matcher_rejects_different_tool_order():
    result = ToolCallMatcher().compare(
        expected=("lookup_recipient", "execute_filter"),
        actual=(
            ToolCall(name="execute_filter"),
            ToolCall(name="lookup_recipient"),
        ),
    )

    assert result.passed is False
    # Score: 2 matched tools + 0 position bonus (execute_filter not last) / 3 total points
    assert result.score == 2 / 3
    assert "execute_filter not in correct position" in result.message


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


def test_mapping_subset_matcher_rejects_unexpected_keys():
    """Test that extra keys in actual are detected as unexpected."""
    result = MappingSubsetMatcher().compare(
        expected={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
        },
        actual={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "extraField": "unexpected",
        },
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "extraField (unexpected)" in result.message


def test_mapping_subset_matcher_rejects_unexpected_nested_keys():
    """Test that extra keys in nested objects are detected as unexpected."""
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

    assert result.passed is False
    assert result.score == 0.0
    assert "time_period.date_type (unexpected)" in result.message
    assert "metadata (unexpected)" in result.message
