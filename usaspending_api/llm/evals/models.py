from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ToolCall:
    """A tool name observed while an assistant ran."""

    name: str

    def as_dict(self) -> str:
        """Return the runtime tool name for reports and comparisons."""
        return self.name


@dataclass(frozen=True)
class EvalCase:
    """
    One ground truth case loaded from the JSON dataset.

    Example of a JSON ground truth case:

        {
            "id": 1,
            "query": "How much did Clark Construction receive in contracts for FY25?",
            "expected_tools": ["lookup_recipient", "execute_filter"],
            "expected_output": {
                "selectedRecipients": ["Clark Construction"],
                "timePeriodType": "fy",
                "timePeriodFY": ["2025"],
                "awardType": ["Contracts"]
            },
            "tags": ["multi_filter", "temporal"],
            "notes": "Similar query to NYT request, variating recipient against q2",
            "approved": true,
            "sme_validation_notes": ""
        }

    Once loaded, it becomes an `EvalCase` with:
        input: the data supplied to the assistant
        expected_tool_calls: expected execution behavior
        expected_output: expected, final normalized filters
        metadata: tags, notes, approval status, and SME context
    """

    name: str
    input: dict[str, Any]
    expected_tool_calls: tuple[str, ...] = ()
    expected_output: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvalObservation:
    """
    The normalized behavior observed after executing an Assistant.

    `output` should contain the final result being evaluated. For example, in 'filter_search',
    this should be a normalized representation of the resulting filters.

    `tool_calls` should preserve the actual tool-call order.
    """

    output: dict[str, Any] = field(default_factory=dict)
    tool_calls: tuple[ToolCall, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MatchResult:
    """
    The results from one comparison.

    An EvalResult (below) contains two MatchResult objects in its implementation.
        - One for tool correctness.
        - One for final filter/output correctness.
    """

    passed: bool
    score: float
    expected: Any
    actual: Any
    message: str


@dataclass(frozen=True)
class EvalResult:
    """
    The complete result for one EvalCase.

    Example score behavior for filter_search:
        - Tool correctness contributes 50%.
        - Final filter correctness contributes 50%.
        - Both must pass for the full case to pass.

    A failed output match with a successful tool match receives a score of 0.5.

    When execution fails (error is not None), the case receives a score of 0.0
    and is marked as failed, but the run continues to preserve all other results.
    """

    case_name: str
    passed: bool
    score: float
    tool_call_match: MatchResult | None = None
    output_match: MatchResult | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class EvalSummary:
    """
    Aggregate results for an entire eval run.

    This is the object used to:
        - Print summary output.
        - Produce JSON for CI.
        - Decide whether `--fail-under` should fail the run.
    """

    assistant: str
    dataset: str
    results: tuple[EvalResult, ...]
    score: float
    passed: bool
    fail_under: float | None = None
    unrun_cases: tuple[EvalCase, ...] = ()
    unrun_reasons: dict[str, str] = field(default_factory=dict)

    @property
    def case_count(self) -> int:
        """Returns the number of cases evaluated."""
        return len(self.results)

    @property
    def passed_count(self) -> int:
        """Returns the number of cases that fully passed."""
        return sum(result.passed for result in self.results)

    @property
    def failed_count(self) -> int:
        """Returns the number of cases that did not fully pass."""
        return self.case_count - self.passed_count

    @property
    def unrun_count(self) -> int:
        """Returns the number of loaded cases excluded from this run."""
        return len(self.unrun_cases)

    @property
    def total_case_count(self) -> int:
        """Returns executed plus excluded cases represented in the report."""
        return self.case_count + self.unrun_count
