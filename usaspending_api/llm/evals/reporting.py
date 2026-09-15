import json
from typing import Any

from usaspending_api.llm.evals.models import EvalResult, EvalSummary


def match_dict(match: Any) -> dict[str, Any] | None:
    """Converts one MatchResult to JSON-compatible data."""
    if match is None:
        return None

    return {
        "passed": match.passed,
        "score": match.score,
        "message": match.message,
        "expected": match.expected,
        "actual": match.actual,
    }


def result_to_dict(result: EvalResult) -> dict[str, Any]:
    """Converts one case result to JSON-compatible data."""
    return {
        "case_name": result.case_name,
        "passed": result.passed,
        "score": result.score,
        "tool_call_match": match_dict(result.tool_call_match),
        "output_match": match_dict(result.output_match),
        "details": result.details,
    }


def summary_to_dict(summary: EvalSummary) -> dict[str, Any]:
    """
    Convert a complete run summary to a machine-readable dict.

    This format can later be saved as a CI artifact or consumed by downstream reporting/tooling without
    parsing human-readable terminal output.
    """
    return {
        "assistant": summary.assistant,
        "dataset": summary.dataset,
        "case_count": summary.case_count,
        "passed_count": summary.passed_count,
        "failed_count": summary.failed_count,
        "score": summary.score,
        "fail_under": summary.fail_under,
        "passed": summary.passed,
        "results": [result_to_dict(result) for result in summary.results],
    }


def render_json(summary: EvalSummary) -> str:
    """Returns a JSON-formatted report."""
    return json.dumps(summary_to_dict(summary), indent=2, sort_keys=True, default=str)


def render_text(summary: EvalSummary) -> str:
    """Returns a concise report intended for local terminal use and CI logs."""
    lines = [
        f"Assistant: {summary.assistant}",
        f"Dataset: {summary.dataset}",
        f"Cases: {summary.case_count}",
        f"Passed: {summary.passed_count}",
        f"Failed: {summary.failed_count}",
        f"Score: {summary.score:.2%}",
    ]

    if summary.fail_under is not None:
        lines.append(f"Required score: {summary.fail_under:.2%}")

    lines.append(f"Result: {'PASS' if summary.passed else 'FAIL'}")
    lines.append("")

    for result in summary.results:
        status = "PASS" if result.passed else "FAIL"
        lines.append(f"{status}  {result.case_name}  ({result.score:.2%})")

        if not result.passed:
            if result.tool_call_match and not result.tool_call_match.passed:
                lines.append(f"  Tool calls: {result.tool_call_match.message}")
            if result.output_match and not result.output_match.passed:
                lines.append(f"  Output: {result.output_match.message}")

    return "\n".join(lines)
