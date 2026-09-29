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
    # Score: 1 correct field (timePeriodType) out of 2 expected = 0.5
    assert result.score == 0.5
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
    # Score: 1 correct field (timePeriodType) out of 2 total = 0.5
    assert result.score == 0.5
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
    # Score: 2 correct fields out of 3 total (2 expected + 1 extra) = 2/3
    assert result.score == 2 / 3
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
    # Score: 2 correct leaf fields out of 4 total (2 expected + 2 extra) = 0.5
    assert result.score == 0.5
    assert "time_period.date_type (unexpected)" in result.message
    assert "metadata (unexpected)" in result.message


def test_mapping_subset_matcher_gives_partial_credit():
    """Test that partial credit is given for partially correct output."""
    result = MappingSubsetMatcher().compare(
        expected={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "awardType": ["Contracts"],
            "selectedRecipients": ["CLARK CONSTRUCTION"],
        },
        actual={
            "timePeriodType": "fy",
            "timePeriodFY": ["2025"],
            "awardType": ["Grants"],  # Wrong value
            # selectedRecipients missing
        },
    )

    assert result.passed is False
    # Score: 2 correct out of 4 expected = 0.5
    assert result.score == 0.5
    assert "awardType" in result.message
    assert "selectedRecipients (missing)" in result.message


def test_mapping_subset_matcher_is_case_insensitive_by_default():
    """Test that string comparisons are case-insensitive by default."""
    result = MappingSubsetMatcher().compare(
        expected={
            "recipient": "Clark Construction",
            "awardType": ["Contracts"],
        },
        actual={
            "recipient": "CLARK CONSTRUCTION",  # Different case
            "awardType": ["contracts"],  # Different case in list
        },
    )

    assert result.passed is True
    assert result.score == 1.0


def test_mapping_subset_matcher_can_be_case_sensitive():
    """Test that case-sensitive mode can be enabled."""
    result = MappingSubsetMatcher(case_sensitive=True).compare(
        expected={
            "recipient": "Clark Construction",
        },
        actual={
            "recipient": "CLARK CONSTRUCTION",  # Different case
        },
    )

    assert result.passed is False
    assert result.score == 0.0
    assert "recipient" in result.message


def test_mapping_subset_matcher_handles_mixed_case_in_lists():
    """Test that case-insensitive comparison works for lists of strings."""
    result = MappingSubsetMatcher().compare(
        expected={
            "recipients": ["Clark Construction", "Boeing"],
        },
        actual={
            "recipients": ["CLARK CONSTRUCTION", "boeing"],
        },
    )

    assert result.passed is True
    assert result.score == 1.0
