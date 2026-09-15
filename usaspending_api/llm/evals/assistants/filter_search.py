from typing import Any

from django.utils import timezone

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.evals.base import BaseEval
from usaspending_api.llm.evals.exceptions import ExecutionError
from usaspending_api.llm.evals.matchers import MappingSubsetMatcher, ToolCallMatcher
from usaspending_api.llm.evals.models import EvalCase, EvalObservation, EvalResult, ToolCall
from usaspending_api.llm.evals.registry import register_eval
from usaspending_api.llm.models.db_models import Assistant, Session, ToolUse
from usaspending_api.llm.tools.execute_filter import build_filter_request, execute_filter_tool
from usaspending_api.llm.tools.lookup_code import lookup_code_tool
from usaspending_api.llm.tools.lookup_location import lookup_location_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool

FILTER_SEARCH_TOOLS = [
    lookup_code_tool,
    lookup_location_tool,
    lookup_recipient_tool,
    execute_filter_tool,
]


def get_active_filter_search_assistant() -> Assistant:
    """Retrieve the active filter-search Assistant."""
    try:
        return Assistant.objects.select_related("ai_model", "system_prompt").get(name="filter-search", is_active=True)
    except Assistant.DoesNotExist as exc:
        raise ExecutionError("Active filter-search Assistant not found.") from exc


def create_eval_session(assistant_config: Assistant) -> Session:
    """
    Create a Session record for a live evaluation execution.

    FilterSearchAssistant.search() persists Message and ToolUse records against a Session. The eval adapter needs to
    read those ToolUse rows to evaluate actual assistant behavior.
    """
    return Session.objects.create(
        ai_model=assistant_config.ai_model,
        tools=[tool.description.name for tool in FILTER_SEARCH_TOOLS],
        system_prompt=assistant_config.system_prompt,
    )


def get_tool_calls(session: Session) -> tuple[ToolCall, ...]:
    """
    Read the tool trace persisted by FilterSearchAssistant.handle_tool_use().

    Ordering first by Message.order preserves the assistant conversation order.
    Ordering by creation time and primary key provides stable ordering for multiple calls associated with a message.
    """
    tool_uses = ToolUse.objects.filter(message__session=session).order_by(
        "message__order",
        "created_at",
        "id",
    )

    return tuple(ToolCall(name=tool_use.name, arguments=tool_use.tool_input) for tool_use in tool_uses)


def get_final_filter_output(session: Session) -> dict[str, Any]:
    """
    Return canonical filters from the last successful execute_filter call.

    execute_filter only returns a hash. Its ToolUse record retains the original arguments sent by the model,
    so the adapter reconstructs the same canonical FilterRequest used by the prod hash generation.
    """
    execute_filter_uses = ToolUse.objects.filter(
        message__session=session, name=FilterSearchAssistant.COMPLETION_TOOL_NAME
    ).order_by(
        "message__order",
        "created_at",
        "id",
    )

    successful_tool_use = next(
        (tool_use for tool_use in reversed(list(execute_filter_uses)) if "error" not in tool_use.result),
        None,
    )

    if successful_tool_use is None:
        raise ExecutionError(f"Session '{session.id}' completed without a successful execute_filter call.")

    try:
        filter_request = build_filter_request(successful_tool_use.tool_input)
    except Exception as exc:
        raise ExecutionError(f"Session '{session.id}' has invalid execute_filter input.") from exc

    return filter_request["filters"]


def run_eval_case(case: EvalCase) -> EvalObservation:
    """
    Execute one real filter-search request through the same assistant class used by FilterSearchViewSet and return
    an evaluation-ready observation.

    The view itself is intentionally not called. Calling the view would require handling API-key behavior, request
    construction, and ND-JSON parsing, while the underlying assistant class provides the same meaningful execution path.
    """
    assistant_config = get_active_filter_search_assistant()
    session = create_eval_session(assistant_config)
    assistant = FilterSearchAssistant(
        assistant=assistant_config,
        tools=FILTER_SEARCH_TOOLS,
        session=session,
    )

    events: list[dict[str, Any]] = []

    try:
        events = list(assistant.search(case.input["query"]))
    except Exception as exc:
        raise ExecutionError(f"Filter Search execution failed for case '{case.name}'.") from exc
    finally:
        session.ended_at = timezone.now()
        session.save(update_fields=["ended_at"])

    error_events = [event for event in events if event.get("type") == "search_error"]

    if error_events:
        raise ExecutionError(
            f"Filter Search execution failed for case '{case.name}': "
            f"{error_events[-1].get('message', 'Unknown search error')}"
        )

    tool_calls = get_tool_calls(session)

    if not tool_calls:
        raise ExecutionError(f"Filter Search case '{case.name}' produced no tool calls.")

    output = get_final_filter_output(session)

    return EvalObservation(
        tool_calls=tool_calls,
        output=output,
        metadata={
            "session_id": str(session.id),
            "assistant_id": assistant_config.id,
            "model_id": assistant_config.ai_model.model_id,
            "tool_use_count": len(tool_calls),
        },
    )


@register_eval("filter_search")
class FilterSearchEval(BaseEval):
    """Evaluator for the Filter Search Assistant."""

    assistant_name = "filter_search"
    default_dataset_name = "ground_truth"

    def __init__(self, *, allow_extra_tool_arguments: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tool_call_matcher = ToolCallMatcher(
            allow_extra_actual_arguments=allow_extra_tool_arguments,
        )
        self.output_matcher = MappingSubsetMatcher()

    def execute(self, case: EvalCase) -> EvalObservation:
        """Execute one actual Filter Search request through the concrete adapter."""
        return run_eval_case(case)

    def evaluate(self, case: EvalCase, observation: EvalObservation) -> EvalResult:
        """Evaluates tool correctness and filter correctness independently."""
        tool_call_match = self.tool_call_matcher.compare(
            case.expected_tool_calls,
            observation.tool_calls,
        )
        output_match = self.output_matcher.compare(
            case.expected_output,
            observation.output,
        )

        return EvalResult(
            case_name=case.name,
            passed=tool_call_match.passed and output_match.passed,
            score=(tool_call_match.score + output_match.score) / 2,
            tool_call_match=tool_call_match,
            output_match=output_match,
            details={
                "case_metadata": case.metadata,
                "execution_metadata": observation.metadata,
            },
        )
