"""
Tests for graceful failure handling in the evaluation framework.
"""

import pytest

from usaspending_api.llm.evals.base import BaseEval
from usaspending_api.llm.evals.exceptions import ExecutionError
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, EvalResult, MatchResult


class MockEval(BaseEval):
    """Test evaluator that can be configured to fail on specific cases."""

    assistant_name = "test_assistant"
    default_dataset_name = "test"

    def __init__(self, fail_on_cases=None, **kwargs):
        super().__init__(**kwargs)
        self.fail_on_cases = fail_on_cases or set()
        self.executed_cases = []

    def execute(self, case: EvalCase) -> EvalObservation:
        """Simulate execution that fails on specific cases."""
        self.executed_cases.append(case.name)
        if case.name in self.fail_on_cases:
            raise ExecutionError(f"Simulated execution failure for {case.name}")
        return EvalObservation(
            output={"result": "success"},
            tool_calls=(),
            metadata={"case_name": case.name},
        )

    def evaluate(self, case: EvalCase, observation: EvalObservation) -> EvalResult:
        """Simple evaluation that always passes if execution succeeded."""
        return EvalResult(
            case_name=case.name,
            passed=True,
            score=1.0,
            tool_call_match=MatchResult(
                passed=True,
                score=1.0,
                expected=[],
                actual=[],
                message="Tools matched",
            ),
            output_match=MatchResult(
                passed=True,
                score=1.0,
                expected={},
                actual=observation.output,
                message="Output matched",
            ),
            details={
                "case_input": case.input,
                "case_metadata": case.metadata,
                "execution_metadata": observation.metadata,
            },
        )


@pytest.fixture
def test_cases():
    """Create test cases for failure handling tests."""
    return [
        EvalCase(
            name="case_1",
            input={"query": "test query 1"},
            expected_tool_calls=(),
            expected_output={},
            metadata={"approved": True, "tags": []},
        ),
        EvalCase(
            name="case_2",
            input={"query": "test query 2"},
            expected_tool_calls=(),
            expected_output={},
            metadata={"approved": True, "tags": []},
        ),
        EvalCase(
            name="case_3",
            input={"query": "test query 3"},
            expected_tool_calls=(),
            expected_output={},
            metadata={"approved": True, "tags": []},
        ),
    ]


def test_single_case_failure_does_not_stop_run(test_cases, monkeypatch):
    """Test that a single case failure doesn't stop the entire run."""

    def mock_load_cases(*args, **kwargs):
        return test_cases

    def mock_load_all_cases(*args, **kwargs):
        return test_cases

    monkeypatch.setattr("usaspending_api.llm.evals.base.load_cases", mock_load_cases)
    monkeypatch.setattr("usaspending_api.llm.evals.base.load_all_cases", mock_load_all_cases)

    # Create evaluator that fails on case_2
    evaluator = MockEval(fail_on_cases={"case_2"}, fail_under=0.5)
    summary = evaluator.run()

    # All three cases should be in results
    assert summary.case_count == 3

    # Two should pass, one should fail
    assert summary.passed_count == 2
    assert summary.failed_count == 1

    # Find the failed case
    failed_results = [r for r in summary.results if not r.passed]
    assert len(failed_results) == 1
    assert failed_results[0].case_name == "case_2"
    assert failed_results[0].score == 0.0
    assert failed_results[0].error is not None
    assert "Simulated execution failure" in failed_results[0].error

    # Successful cases should have no error
    passed_results = [r for r in summary.results if r.passed]
    assert len(passed_results) == 2
    for result in passed_results:
        assert result.error is None
        assert result.score == 1.0

    # All cases should have been attempted
    assert set(evaluator.executed_cases) == {"case_1", "case_2", "case_3"}


def test_multiple_case_failures(test_cases, monkeypatch):
    """Test that multiple case failures are all recorded."""

    def mock_load_cases(*args, **kwargs):
        return test_cases

    def mock_load_all_cases(*args, **kwargs):
        return test_cases

    monkeypatch.setattr("usaspending_api.llm.evals.base.load_cases", mock_load_cases)
    monkeypatch.setattr("usaspending_api.llm.evals.base.load_all_cases", mock_load_all_cases)

    # Create evaluator that fails on case_1 and case_3
    evaluator = MockEval(fail_on_cases={"case_1", "case_3"}, fail_under=0.0)
    summary = evaluator.run()

    # All three cases should be in results
    assert summary.case_count == 3

    # One should pass, two should fail
    assert summary.passed_count == 1
    assert summary.failed_count == 2

    # Check failed cases
    failed_results = [r for r in summary.results if not r.passed]
    assert len(failed_results) == 2
    failed_names = {r.case_name for r in failed_results}
    assert failed_names == {"case_1", "case_3"}

    # All failed cases should have error messages
    for result in failed_results:
        assert result.error is not None
        assert result.score == 0.0


def test_all_cases_fail(test_cases, monkeypatch):
    """Test behavior when all cases fail."""

    def mock_load_cases(*args, **kwargs):
        return test_cases

    def mock_load_all_cases(*args, **kwargs):
        return test_cases

    monkeypatch.setattr("usaspending_api.llm.evals.base.load_cases", mock_load_cases)
    monkeypatch.setattr("usaspending_api.llm.evals.base.load_all_cases", mock_load_all_cases)

    # Create evaluator that fails on all cases
    evaluator = MockEval(fail_on_cases={"case_1", "case_2", "case_3"}, fail_under=0.0)
    summary = evaluator.run()

    # All three cases should be in results
    assert summary.case_count == 3

    # All should fail
    assert summary.passed_count == 0
    assert summary.failed_count == 3

    # Score should be 0.0
    assert summary.score == 0.0

    # All results should have errors
    for result in summary.results:
        assert not result.passed
        assert result.error is not None
        assert result.score == 0.0


def test_no_cases_fail(test_cases, monkeypatch):
    """Test normal behavior when no cases fail."""

    def mock_load_cases(*args, **kwargs):
        return test_cases

    def mock_load_all_cases(*args, **kwargs):
        return test_cases

    monkeypatch.setattr("usaspending_api.llm.evals.base.load_cases", mock_load_cases)
    monkeypatch.setattr("usaspending_api.llm.evals.base.load_all_cases", mock_load_all_cases)

    # Create evaluator that doesn't fail on any cases
    evaluator = MockEval(fail_on_cases=set(), fail_under=0.9)
    summary = evaluator.run()

    # All three cases should be in results
    assert summary.case_count == 3

    # All should pass
    assert summary.passed_count == 3
    assert summary.failed_count == 0

    # Score should be 1.0
    assert summary.score == 1.0

    # No results should have errors
    for result in summary.results:
        assert result.passed
        assert result.error is None
        assert result.score == 1.0


def test_unexpected_exception_is_caught(test_cases, monkeypatch):
    """Test that unexpected exceptions (not ExecutionError) are also caught gracefully."""

    def mock_load_cases(*args, **kwargs):
        return test_cases

    def mock_load_all_cases(*args, **kwargs):
        return test_cases

    monkeypatch.setattr("usaspending_api.llm.evals.base.load_cases", mock_load_cases)
    monkeypatch.setattr("usaspending_api.llm.evals.base.load_all_cases", mock_load_all_cases)

    class FailingEval(MockEval):
        def execute(self, case: EvalCase) -> EvalObservation:
            if case.name == "case_2":
                raise ValueError("Unexpected error!")
            return super().execute(case)

    evaluator = FailingEval(fail_under=0.0)
    summary = evaluator.run()

    # All three cases should be in results
    assert summary.case_count == 3

    # Two should pass, one should fail
    assert summary.passed_count == 2
    assert summary.failed_count == 1

    # Find the failed case
    failed_results = [r for r in summary.results if not r.passed]
    assert len(failed_results) == 1
    assert failed_results[0].case_name == "case_2"
    assert failed_results[0].error is not None
    assert "ValueError" in failed_results[0].error
    assert "Unexpected error!" in failed_results[0].error
