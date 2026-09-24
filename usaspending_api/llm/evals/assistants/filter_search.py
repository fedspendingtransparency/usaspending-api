import logging
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
from usaspending_api.llm.tools.lookup_agency import lookup_agency_tool
from usaspending_api.llm.tools.lookup_code import lookup_code_tool
from usaspending_api.llm.tools.lookup_location import lookup_location_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool

logger = logging.getLogger(__name__)

FILTER_SEARCH_TOOLS = [
    lookup_agency_tool,
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

    return tuple(ToolCall(name=tool_use.name) for tool_use in tool_uses)


def get_final_filter_output(session: Session) -> dict[str, Any]:
    """
    Return canonical filters from the last successful execute_filter call.

    execute_filter only returns a hash. Its ToolUse record retains the filter payload used by the model,
    so the adapter reconstructs the same canonical FilterRequest used by production hash generation.
    """
    execute_filter = (
        ToolUse.objects.filter(message__session=session, name=FilterSearchAssistant.COMPLETION_TOOL_NAME)
        .exclude(result__contains="error")
        .order_by(
            "message__order",
            "created_at",
        )
        .last()
    )

    if not execute_filter:
        logger.error(f"Session '{session.id}' completed without a successful execute_filter call.")
        return {}

    try:
        filter_request = build_filter_request(execute_filter.tool_input)
    except Exception as exc:
        logger.error(f"Session '{session.id}' has invalid execute_filter input: {exc}")
        return {}

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
        logger.exception(f"Filter Search execution failed for case '{case.name}': {exc}")
    finally:
        session.ended_at = timezone.now()
        session.save(update_fields=["ended_at"])

    error_events = [event for event in events if event.get("type") == "search_error"]

    if error_events:
        logger.error(
            f"Filter Search execution failed for case '{case.name}': "
            f"{error_events[-1].get('message', 'Unknown search error')}"
        )

    tool_calls = get_tool_calls(session)

    if not tool_calls:
        logger.error(f"Filter Search case '{case.name}' produced no tool calls.")

    output = get_final_filter_output(session)

    inference_config = assistant_config.inference_config or {}

    return EvalObservation(
        tool_calls=tool_calls,
        output=output,
        metadata={
            "session_id": str(session.id),
            "assistant": assistant_config.name,
            "assistant_id": assistant_config.id,
            "ai_model_id": assistant_config.ai_model.model_id,
            "system_prompt_id": assistant_config.system_prompt_id,
            "inference_config_temp": inference_config.get("temperature"),
            "inference_config_top_p": inference_config.get("topP", inference_config.get("top_p")),
            "inference_config_max_tokens": inference_config.get("maxTokens", inference_config.get("max_tokens")),
            "inference_config_stop_sequences": inference_config.get(
                "stopSequences",
                inference_config.get("stop_sequences", []),
            ),
            "tool_use_count": len(tool_calls),
        },
    )


@register_eval("filter_search")
class FilterSearchEval(BaseEval):
    """Evaluator for the Filter Search Assistant."""

    assistant_name = "filter_search"
    default_dataset_name = "config"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tool_call_matcher = ToolCallMatcher()
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
                "case_input": case.input,
                "execution_metadata": observation.metadata,
            },
        )
