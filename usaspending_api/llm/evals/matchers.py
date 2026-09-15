from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from usaspending_api.llm.evals.models import MatchResult, ToolCall, ToolExpectation


@dataclass(frozen=True)
class ToolCallMatcher:
    """
    Compares expected tool calls to actual observed tool calls.

    Initial behavior is intentionally strict:
        - Tool count must match.
        - Order must match.
        - Names must match.
        - Arguments are checked only when the CSV eventually supplies them.

    NOTE: `allow_extra_actual_arguments` supports future argument-level ground truth data
    where expected arguments are a required subset rather than the complete actual argument mapping.
    """

    allow_extra_actual_arguments: bool = False

    def compare(self, expected: Sequence[ToolExpectation], actual: Sequence[ToolCall]) -> MatchResult:
        expected_data = [tool.as_dict() for tool in expected]
        actual_data = [tool.as_dict() for tool in actual]

        if len(expected) != len(actual):
            return MatchResult(
                passed=False,
                score=0.0,
                expected=expected_data,
                actual=actual_data,
                message=f"Expected {len(expected)} tool call(s), received {len(actual)}.",
            )

        for index, (expected_tool, actual_tool) in enumerate(zip(expected, actual, strict=True)):
            if expected_tool.name != actual_tool.name:
                return MatchResult(
                    passed=False,
                    score=0.0,
                    expected=expected_data,
                    actual=actual_data,
                    message=f"Tool call {index} expected '{expected_tool.name}', received '{actual_tool.name}'.",
                )

            if expected_tool.arguments is not None and expected_tool.arguments != actual_tool.arguments:
                return MatchResult(
                    passed=False,
                    score=0.0,
                    expected=expected_data,
                    actual=actual_data,
                    message=f"Tool arguments differ for '{expected_tool.name}' at position '{index}'.",
                )

        return MatchResult(
            passed=True,
            score=1.0,
            expected=expected_data,
            actual=actual_data,
            message="Tool call sequence matches expected values.",
        )


@dataclass(frozen=True)
class MappingSubsetMatcher:
    """
    Compares expected output to actual output as a recursive mapping subset.

    Expected values must exist and match in actual output. Extra actual fields are allowed.

    This should allow non-semantic details like request IDs, model metadata, or additional resolved filter data
    that does not need to be authored in every ground truth row to exist in production output without breaking
    the eval framework.
    """

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
        """
        Recursively identifies missing or mismatched expected values.

        Example failure:

            time_period.fiscal_year
            (expected 2025, received 2024)
        """
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

        return differences
