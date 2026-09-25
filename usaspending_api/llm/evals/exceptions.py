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
        - The JSON file does not exist or is malformed.
        - A required field is missing.
        - A case ID is duplicated.
        - A case field has an invalid type or value.
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
        - No execution adapter was configured.
        - The configured dotted Python path cannot be imported.
        - The adapter returns an invalid observation object.
        - The assistant raises an unexpected exception during execution.
    """
