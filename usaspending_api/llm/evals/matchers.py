from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from usaspending_api.llm.evals.models import MatchResult, ToolCall


@dataclass(frozen=True)
class ToolCallMatcher:
    """Compare expected and observed tool names in order."""

    def compare(self, expected: Sequence[str], actual: Sequence[ToolCall]) -> MatchResult:
        expected_data = list(expected)
        actual_data = [tool.name for tool in actual]
        mismatch = self._find_mismatch(expected_data, actual_data)

        return MatchResult(
            passed=mismatch is None,
            score=1.0 if mismatch is None else 0.0,
            expected=expected_data,
            actual=actual_data,
            message=mismatch or "Tool call sequence matches expected values.",
        )

    def _find_mismatch(self, expected: Sequence[str], actual: Sequence[str]) -> str | None:
        mismatch = None

        if len(expected) != len(actual):
            mismatch = f"Expected {len(expected)} tool call(s), received {len(actual)}."
        else:
            for index, (expected_name, actual_name) in enumerate(zip(expected, actual, strict=True)):
                if expected_name != actual_name:
                    mismatch = f"Tool call {index} expected '{expected_name}', received '{actual_name}'."
                    break

        return mismatch


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

        return differences
