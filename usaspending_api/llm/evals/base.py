import logging
from abc import ABC, abstractmethod
from pathlib import Path
from statistics import fmean

from usaspending_api.llm.evals.exceptions import ExecutionError
from usaspending_api.llm.evals.loader import load_all_cases, load_cases
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, EvalResult, EvalSummary

logger = logging.getLogger(__name__)


class BaseEval(ABC):
    """
    Generic assistant-evaluation lifecycle class.

    Subclasses (placed in `llm/evals/assistants`) must define:
        - assistant_name: registry/command name.
        - default_dataset_name: JSON dataset without file extension.
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
        incremental_output: Path | None = None,
    ) -> None:
        if not self.assistant_name:
            raise ValueError("assistant_name must be defined.")
        if not self.default_dataset_name:
            raise ValueError("default_dataset_name must be defined.")
        if fail_under is not None and not 0.0 <= fail_under <= 1.0:
            raise ValueError("fail_under must be between 0.0 and 1.0.")

        self.dataset_name = dataset_name or self.default_dataset_name
        self.selected_case_names = selected_case_names
        self.include_unapproved = include_unapproved
        self.tags = tags
        self.fail_under = fail_under
        self.incremental_output = incremental_output

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
        """Runs the assistant and returns a normalized observation."""
        pass

    @abstractmethod
    def evaluate(self, case: EvalCase, observation: EvalObservation) -> EvalResult:
        """Compares one observed execution against its ground truth."""
        pass

    def _execute_case_safely(self, case: EvalCase) -> EvalResult:
        """
        Execute and evaluate one case, catching errors gracefully.

        If execution or evaluation fails, return a failed EvalResult with error details
        rather than propagating the exception. This allows the run to continue and
        preserve results from all other cases.
        """
        try:
            observation = self.execute(case)
            return self.evaluate(case, observation)
        except ExecutionError as exc:
            logger.error(f"Execution failed for case '{case.name}': {exc}")
            return EvalResult(
                case_name=case.name,
                passed=False,
                score=0.0,
                error=str(exc),
            )
        except Exception as exc:
            logger.exception(f"Unexpected error evaluating case '{case.name}'")
            return EvalResult(
                case_name=case.name,
                passed=False,
                score=0.0,
                error=f"Unexpected error: {type(exc).__name__}: {exc}",
            )

    def _write_incremental_result(self, index: int, total: int, case: EvalCase, result: EvalResult) -> None:
        """Write a single case result to the incremental output file."""
        try:
            with self.incremental_output.open("a", encoding="utf-8") as f:
                f.write(f"[{index}/{total}] Case: {case.name}\n")
                f.write(f"  Status: {'PASS' if result.passed else ('ERROR' if result.error else 'FAIL')}\n")
                f.write(f"  Score: {result.score:.4f}\n")

                if result.error:
                    f.write(f"  Error: {result.error}\n")
                else:
                    if result.tool_call_match:
                        f.write(f"  Tool Score: {result.tool_call_match.score:.4f}\n")
                        if not result.tool_call_match.passed:
                            f.write(f"  Tool Message: {result.tool_call_match.message}\n")
                    if result.output_match:
                        f.write(f"  Output Score: {result.output_match.score:.4f}\n")
                        if not result.output_match.passed:
                            f.write(f"  Output Message: {result.output_match.message}\n")

                f.write("\n")
        except Exception as exc:
            logger.warning(f"Failed to write incremental result for case '{case.name}': {exc}")

    def run(self) -> EvalSummary:
        """
        Executes selected cases while retaining excluded cases for reporting.

        The summary score is calculated only from cases that actually ran.
        Excluded cases are represented separately as unrun_cases.

        Individual case failures are caught and recorded as failed results with
        error messages, allowing the run to continue and preserve all results.

        Progress is logged incrementally so results are not lost if the run fails.
        """
        all_cases = load_all_cases(self.dataset_name)
        cases = self.load_cases()

        if not cases:
            raise ValueError(f"Dataset `{self.dataset_name}` does not contain evaluation cases.")

        logger.info(f"Starting evaluation: {len(cases)} case(s) to run")

        # Prepare incremental output file if specified
        if self.incremental_output:
            self.incremental_output.parent.mkdir(parents=True, exist_ok=True)
            # Write header
            with self.incremental_output.open("w", encoding="utf-8") as f:
                f.write(f"# Incremental evaluation results for {self.assistant_name}\n")
                f.write(f"# Dataset: {self.dataset_name}\n")
                f.write(f"# Total cases: {len(cases)}\n\n")

        # Execute cases one by one with progress logging
        results = []
        for i, case in enumerate(cases, start=1):
            logger.info(f"[{i}/{len(cases)}] Running case: {case.name}")
            result = self._execute_case_safely(case)

            # Log immediate result
            status = "PASS" if result.passed else ("ERROR" if result.error else "FAIL")
            logger.info(f"[{i}/{len(cases)}] Case '{case.name}': {status} (score: {result.score:.2f})")

            if result.error:
                logger.error(f"  Error: {result.error}")
            elif not result.passed:
                if result.tool_call_match and not result.tool_call_match.passed:
                    logger.warning(f"  Tool calls: {result.tool_call_match.message}")
                if result.output_match and not result.output_match.passed:
                    logger.warning(f"  Output: {result.output_match.message}")

            results.append(result)

            # Write incremental result to file
            if self.incremental_output:
                self._write_incremental_result(i, len(cases), case, result)

        results = tuple(results)
        run_case_names = {case.name for case in cases}
        unrun_cases = tuple(case for case in all_cases if case.name not in run_case_names)
        unrun_reasons = {
            case.name: _unrun_reason(
                case,
                selected_case_names=self.selected_case_names,
                include_unapproved=self.include_unapproved,
                tags=self.tags,
            )
            for case in unrun_cases
        }
        score = fmean(result.score for result in results)
        passed = self.fail_under is None or score >= self.fail_under

        logger.info(f"Evaluation complete: {len(results)} case(s) run, score: {score:.2%}")

        return EvalSummary(
            assistant=self.assistant_name,
            dataset=self.dataset_name,
            results=results,
            score=score,
            passed=passed,
            fail_under=self.fail_under,
            unrun_cases=unrun_cases,
            unrun_reasons=unrun_reasons,
        )


def _unrun_reason(
    case: EvalCase,
    *,
    selected_case_names: set[str] | None,
    include_unapproved: bool,
    tags: set[str] | None,
) -> str:
    """Explains why a validated case was excluded from an evaluation run."""
    reason = "excluded by the run filters"

    if not include_unapproved and not case.metadata["approved"]:
        reason = "case is not approved"
    elif selected_case_names and case.name not in selected_case_names:
        reason = "case was not selected"
    elif tags and not tags.intersection(case.metadata["tags"]):
        reason = "case did not match the selected tags for this run"

    return reason
