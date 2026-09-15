from importlib import import_module
from typing import Any, Callable, Mapping, Sequence

from django.conf import settings

from usaspending_api.llm.evals.exceptions import ExecutionError
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, ToolCall

# An adapter receives a normalized EvalCase and returns a normalized EvalObservation.
AssistantExecutor = Callable[[EvalCase], EvalObservation]


def import_string(dotted_path: str) -> Any:
    """
    Import a Python object from a dotted path.

    Example:
        llm.some_module.run_eval_case

    This allows Django settings or --executor to reference a production adapter without hard-coding a specific
    assistant implementation into the general eval framework.
    """
    module_path, separator, attribute_name = dotted_path.rpartition(".")

    if not separator or not module_path or not attribute_name:
        raise ExecutionError(f"Executor path '{dotted_path}' must use '<module>.<attribute>' format.")

    try:
        module = import_module(module_path)
        return getattr(module, attribute_name)
    except (ImportError, AttributeError) as exc:
        raise ExecutionError(f"Unable to import evaluation executor '{dotted_path}'.") from exc


def normalize_tool_calls(raw_tool_calls: Any) -> tuple[ToolCall, ...]:
    """
    Normalize tool-call data returned by an execution adapter.

    The adapter may return:
        - ToolCall objects, or
        - Mappings with `name` and `arguments` keys.
    """
    if raw_tool_calls is None:
        return ()

    if not isinstance(raw_tool_calls, Sequence) or isinstance(raw_tool_calls, (str, bytes)):
        raise ExecutionError("Executor result field 'tool_calls' must be a list.")

    normalized_calls: list[ToolCall] = []

    for index, raw_call in enumerate(raw_tool_calls):
        if isinstance(raw_call, ToolCall):
            normalized_calls.append(raw_call)
            continue

        if not isinstance(raw_call, Mapping):
            raise ExecutionError(f"tool_calls[{index}] must be a ToolCall or mapping.")

        name = raw_call.get("name", {})
        arguments = raw_call.get("arguments", {})

        if not isinstance(name, str) or not name:
            raise ExecutionError(f"tool_calls[{index}].name must be a non-empty string.")

        if not isinstance(arguments, Mapping):
            raise ExecutionError(f"tool_calls[{index}].arguments must be a mapping.")

        normalized_calls.append(ToolCall(name=name, arguments=dict(arguments)))

    return tuple(normalized_calls)


def normalize_observation(raw_result: Any) -> EvalObservation:
    """
    Normalizes an adapter return value into an EvalObservation object.

    An adapter may return an EvalObservation directly, which is preferred, or a plain mapping such as:

        {
            "tool_calls": [
                {
                    "name": "recipient",
                    "arguments": {"recipient": "Clark Construction"},
                },
            ],
            "output": {
                "recipient": "Clark Construction",
            },
            "metadata": {
                "request_id": "example",
            },
        }

    This helps convert that mapping into the EvalObservation custom object.
    """
    if isinstance(raw_result, EvalObservation):
        return raw_result

    if not isinstance(raw_result, Mapping):
        raise ExecutionError(
            "The execution adapter must return EvalObservation or a mapping containing 'output', 'tool_calls', and "
            "optional 'metadata' keys."
        )

    output = raw_result.get("output", {})
    tool_calls = raw_result.get("tool_calls", [])
    metadata = raw_result.get("metadata", {})

    if not isinstance(output, Mapping):
        raise ExecutionError("Executor result field 'output' must be a mapping.")

    if not isinstance(metadata, Mapping):
        raise ExecutionError("Executor result field 'metadata' must be a mapping.")

    return EvalObservation(
        output=dict(output),
        tool_calls=normalize_tool_calls(tool_calls),
        metadata=dict(metadata),
    )


def resolve_executor(assistant_name: str, override_path: str | None = None) -> AssistantExecutor:
    """
    Resolves the adapter used to execute an Assistant.

    Normal configuration is supplied by Django settings:

        LLM_EVAL_ASSISTANT_EXECUTORS = {
            "filter_search": "llm.some_module.run_eval_case",
        }

    For local tests, this allows users to input --executor to override the configured path for one command invocation.
    """
    configured_executors = getattr(settings, "LLM_EVAL_EXECUTORS", {})
    executor_path = override_path or configured_executors.get(assistant_name)

    if not executor_path:
        raise ExecutionError(
            f"No execution adapter configured for assistant '{assistant_name}'. "
            "Set LLM_EVAL_EXECUTORS or pass --executor."
        )

    executor = import_string(executor_path)

    if not callable(executor):
        raise ExecutionError(
            f"Evaluation executor '{executor_path}' for '{assistant_name}' is not callable."
        )

    def execute(case: EvalCase) -> EvalObservation:
        """
        Runs the configured adapter and normalizes its results.

        Unexpected adapter exceptions are wrapped in ExecutionError so the command reports which assistant
        and dataset case failed.
        """
        try:
            return normalize_observation(executor(case))
        except ExecutionError as exc:
            raise ExecutionError(f"Execution failed for: {case.name}") from exc
        except Exception as exc:
            raise ExecutionError(
                f"Assistant '{assistant_name}' failed while evaluating case '{case.name}'."
            ) from exc

    return execute