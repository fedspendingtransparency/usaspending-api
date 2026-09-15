from collections.abc import Callable
from typing import TypeVar

from usaspending_api.llm.evals.base import BaseEval
from usaspending_api.llm.evals.exceptions import EvalRegistrationError

# Describes the class object inheriting from BaseEval.
EvalClass = TypeVar("EvalClass", bound=type[BaseEval])

# This is populated when the assistant evaluator modules are imported.
EVAL_REGISTRY: dict[str, type[BaseEval]] = {}


def register_eval(name: str) -> Callable[[EvalClass], EvalClass]:
    """
    Return a class decorator that registers an evaluator.

    Usage example from llm.evals.assistants.filter_search:

        @register_eval("filter_search")
        class FilterSearchEval(BaseEval):
            ...
    """
    normalized_name = name.strip().lower()

    if not normalized_name:
        raise EvalRegistrationError("An evaluator must have a non-empty registry name.")

    def decorator(evaluator_class: EvalClass) -> EvalClass:
        if normalized_name in EVAL_REGISTRY:
            raise EvalRegistrationError(
                f"An evaluator is already registered as '{normalized_name}'."
            )

        EVAL_REGISTRY[normalized_name] = evaluator_class
        return evaluator_class

    return decorator


def load_builtin_evaluators() -> None:
    """
    Imports project-supported evaluator modules.

    Example:
        - Importing filter_search executes its @register_eval("filter_search") decorator, which adds FilterSearchEval
            to the EVAL_REGISTRY.

    This import is placed here to avoid circular imports:

        registry.py imports filter_search.py
        filter_search.py imports register_eval from registry.py

    By the time this is called, registry.py should have finished defining EVAL_REGISTRY and register_eval.

    NOTE: Because this is not used anywhere in this file, it is ignored with noqa: F401.
    """
    from llm.evals.assistants import filter_search # noqa: F401


def get_eval_class(name: str) -> type[BaseEval]:
    """
    Returns an evaluator class by its command-line name.

    Example:
        get_eval_class("filter_search") -> FilterSearchEval
    """
    load_builtin_evaluators()
    normalized_name = name.strip().lower()

    try:
        return EVAL_REGISTRY[normalized_name]
    except KeyError as exc:
        available_names = ", ".join(sorted(EVAL_REGISTRY)) or "none"
        raise EvalRegistrationError(f"Unknown assistant '{name}'. Available assistants: {available_names}.") from exc


def registered_assistant_names() -> tuple[str, ...]:
    """Returns names used to populate the management command's --assistant choices."""
    load_builtin_evaluators()
    return tuple(sorted(EVAL_REGISTRY))
