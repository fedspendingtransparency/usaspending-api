from abc import ABC, abstractmethod
from statistics import fmean

from usaspending_api.llm.evals.loader import load_cases
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, EvalResult, EvalSummary

class BaseEval(ABC):
    """
    Generic assistant-evaluation lifecycle class.

    Subclasses (placed in `llm/evals/assistants`) must define:
        - assistant_name: registry/command name.
        - default_dataset_name: CSV dataset without file extension (currently hard-coded to only accept CSV).
        - execute(): how to run the assistant.
        - evaluate(): what 'correctness' means for that assistant.
    """
    assistant_name: str
    default_dataset_name: str

    def __init__(
            self,
            *,
            dataset_name: str | None = None,
            selected_case_names: set[str] | None = None,
            include_unapproved: bool = False,
            tags: set[str] | None = None,
            fail_under: float | None = None,
    ) -> None:
        if not self.assistant_name:
            raise ValueError("assistant_name must be defined.")
        if not self.default_dataset_name:
            raise ValueError("default_dataset_name must be defined.")
        if fail_under is not None and 0.0 <= fail_under <= 1.0:
            raise ValueError("fail_under must be between 0 and 1.0.")

        self.dataset_name = dataset_name or self.default_dataset_name
        self.selected_case_names = selected_case_names
        self.include_unapproved = include_unapproved
        self.tags = tags
        self.fail_under = fail_under

    def load_cases(self) -> list[EvalCase]:
        """
        Loads dataset cases using the command's selection options.

        This method is generic because every assistant evaluator may use its own:
            - Named dataset.
            - Individual case selection.
            - Approved/unapproved filtering set.
            - Tag filters.
        """
        return load_cases(
            self.dataset_name,
            selected_case_names=self.selected_case_names,
            include_unapproved=self.include_unapproved,
            tags=self.tags,
        )

    @abstractmethod
    def execute(self, case: EvalCase) -> EvalObservation:
        """Run the assistant and return a normalized observation."""

    @abstractmethod
    def evaluate(self, case: EvalCase, observation: EvalObservation) -> EvalResult:
        """Compare one observed execution against its ground truth."""

    def run(self) -> EvalSummary:
        """
        Execute the complete generic lifecycle.

        The summary score is the average of the per-case scores.

        In the initial filter_search evaluator:
            - Full pass = 1.0
            - Tool-only or output-only pass = 0.5
            - Both fail = 0.0
        """
        cases = self.load_cases()

        if not cases:
            raise ValueError(
                f"Dataset `{self.dataset_name}` does not contain evaluation cases."
            )

        results = tuple(self.evaluate(case, self.execute(case)) for case in cases)
        score = fmean(result.score for result in results)
        passed = self.fail_under is None or score >= self.fail_under

        return EvalSummary(
            assistant=self.assistant_name,
            dataset=self.dataset_name,
            results=results,
            score=score,
            passed=passed,
            fail_under=self.fail_under,
        )