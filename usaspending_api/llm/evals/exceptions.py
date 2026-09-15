class EvalError(Exception):
    """
    Base exception class for evaluation framework failures.

    The management command can catch this one parent type rather than needing to know
    every evaluation failure type individually.
    """


class DatasetError(EvalError):
    """
    Raised when there is an issue with the dataset (i.e., ground truth) used for evaluation.

    Examples:
        - The CSV file does not exist.
        - A required header is missing.
        - A case ID is duplicated.
        - "expected_output" does not use the required "key = value" format.
    """


class EvalRegistrationError(EvalError):
    """
    Raised when an evaluator is missing or registered incorrectly.

    Example:
        - python manage.py run_llm_eval --assistant some_unknown_assistant
    """


class ExecutionError(EvalError):
    """
    Raised when an assistant cannot produce an evaluation observation.

    Examples:
        - No execution adapter was configured in Django settings.
        - The configured dotted Python path cannot be imported.
        - The adapter returns an invalid observation object.
        - The assistant raises an unexpected exception during execution.
    """