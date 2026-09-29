from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from usaspending_api.llm.evals.models import MatchResult, ToolCall


@dataclass
class _MatchContext:
    """Internal context for building match messages."""

    passed: bool
    missing: set[str]
    extra: set[str]
    matches: int
    expected_count: int
    execute_filter_correct_position: bool
    actual_data: list[str]


@dataclass(frozen=True)
class ToolCallMatcher:
    """
    Compare expected and observed tool names using set-based matching with position scoring.

    Order is unimportant and duplicates are ignored. However, execute_filter position
    contributes to the score: it should be called last.

    Scoring:
    - Set matching: (matched tools / expected tools)
    - Position bonus: +1 if execute_filter is last (when present)
    - Total possible points: expected tool count + 1 (for execute_filter position)
    - Final score: (matched tools + position bonus) / total possible points

    Example:
        Expected: [lookup_recipient, lookup_awards, execute_filter]
        Actual: [execute_filter, lookup_recipient, lookup_awards, lookup_awards]
        Score: 3/4 = 0.75 (all 3 tools matched, despite duplicates, but execute_filter was not last)
    """

    def compare(self, expected: Sequence[str], actual: Sequence[ToolCall]) -> MatchResult:
        expected_data = list(expected)
        actual_data = [tool.name for tool in actual]
        expected_set = set(expected_data)
        actual_set = set(actual_data)

        # Determine which result type to create.
        if not expected_set and not actual_set:
            result = self._create_perfect_match(expected_data, actual_data)
        elif not expected_set:
            result = self._create_unexpected_tools_result(expected_data, actual_data, actual_set)
        elif not actual_set:
            result = self._create_no_tools_called_result(expected_data, actual_data, expected_set)
        else:
            # Normal case: both expected and actual have tools.
            result = self._create_partial_match_result(expected_data, actual_data, expected_set, actual_set)

        return result

    def _create_perfect_match(self, expected_data: list[str], actual_data: list[str]) -> MatchResult:
        """Create result for when no tools are expected or called."""
        return MatchResult(
            passed=True,
            score=1.0,
            expected=expected_data,
            actual=actual_data,
            message="Tool call sequence matches expected values.",
        )

    def _create_unexpected_tools_result(
        self, expected_data: list[str], actual_data: list[str], actual_set: set[str]
    ) -> MatchResult:
        """Create result for when tools were called but none were expected."""
        return MatchResult(
            passed=False,
            score=0.0,
            expected=expected_data,
            actual=actual_data,
            message=f"Expected no tools, but {len(actual_set)} tool(s) were called: {sorted(actual_set)}",
        )

    def _create_no_tools_called_result(
        self, expected_data: list[str], actual_data: list[str], expected_set: set[str]
    ) -> MatchResult:
        """Create result for when tools were expected but none were called."""
        return MatchResult(
            passed=False,
            score=0.0,
            expected=expected_data,
            actual=actual_data,
            message=f"Expected {len(expected_set)} tool(s), but none were called.",
        )

    def _create_partial_match_result(
        self, expected_data: list[str], actual_data: list[str], expected_set: set[str], actual_set: set[str]
    ) -> MatchResult:
        """Create result for normal case with partial or full matches."""
        matches = len(expected_set & actual_set)
        missing = expected_set - actual_set
        extra = actual_set - expected_set

        # Check execute_filter position and calculate score.
        execute_filter_correct_position = self._is_execute_filter_last(actual_data)
        position_bonus = 1 if execute_filter_correct_position else 0
        total_possible_points = len(expected_set) + 1
        score = (matches + position_bonus) / total_possible_points
        passed = score == 1.0 and not extra

        context = _MatchContext(
            passed=passed,
            missing=missing,
            extra=extra,
            matches=matches,
            expected_count=len(expected_set),
            execute_filter_correct_position=execute_filter_correct_position,
            actual_data=actual_data,
        )
        message = self._build_match_message(context)

        return MatchResult(
            passed=passed,
            score=score,
            expected=expected_data,
            actual=actual_data,
            message=message,
        )

    def _build_match_message(self, context: _MatchContext) -> str:
        """Build a descriptive message about the match result."""
        if context.passed:
            return "Tool call sequence matches expected values."

        parts = []
        if context.missing:
            parts.append(f"Missing tools: {sorted(context.missing)}")
        if context.extra:
            parts.append(f"Unexpected tools: {sorted(context.extra)}")
        if context.matches > 0:
            parts.append(f"Matched {context.matches}/{context.expected_count} expected tools")
        if not context.execute_filter_correct_position:
            if "execute_filter" in context.actual_data:
                parts.append("execute_filter not in correct position (should be last)")
            else:
                parts.append("execute_filter not called")

        return "; ".join(parts) + "."

    def _is_execute_filter_last(self, actual: list[str]) -> bool:
        """
        Check if execute_filter is called last.

        Returns True if execute_filter is the last tool called, False otherwise.
        """
        COMPLETION_TOOL = "execute_filter"

        if COMPLETION_TOOL not in actual:
            return False

        # Check if execute_filter is the last tool called.
        execute_filter_index = len(actual) - 1 - actual[::-1].index(COMPLETION_TOOL)
        return execute_filter_index == len(actual) - 1


@dataclass(frozen=True)
class MappingSubsetMatcher:
    """Compare expected output to actual output as a recursive mapping subset."""

    def compare(self, expected: Mapping[str, Any], actual: Mapping[str, Any]) -> MatchResult:
        differences = self._find_differences(expected=expected, actual=actual)

        return MatchResult(
            passed=not differences,
            score=1.0 if not differences else 0.0,
            expected=dict(expected),
            actual=dict(actual),
            message=(
                "All expected output values are present."
                if not differences
                else f"Output differs at: {', '.join(differences)}"
            ),
        )

    def _find_differences(self, expected: Mapping[str, Any], actual: Mapping[str, Any], path: str = "") -> list[str]:
        differences: list[str] = []

        for key, expected_value in expected.items():
            current_path = f"{path}.{key}" if path else key

            if key not in actual:
                differences.append(f"{current_path} (missing)")
                continue

            actual_value = actual[key]

            if isinstance(expected_value, Mapping):
                if not isinstance(actual_value, Mapping):
                    differences.append(f"{current_path} (expected nested mapping)")
                    continue

                differences.extend(
                    self._find_differences(
                        expected=expected_value,
                        actual=actual_value,
                        path=current_path,
                    )
                )
                continue

            if expected_value != actual_value:
                differences.append(f"{current_path} (expected {expected_value!r}, received {actual_value!r})")

        # Check for unexpected extra keys in actual.
        for key in actual.keys():
            if key not in expected:
                current_path = f"{path}.{key}" if path else key
                differences.append(f"{current_path} (unexpected)")

        return differences
