import logging
from typing import Any, AsyncGenerator

from django.db.models import Sum
from django.http import HttpRequest, StreamingHttpResponse
from django.utils import timezone
from ninja import Router

from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import Assistant, Session, ToolUse
from usaspending_api.llm.models.py_models import FilterSearchEvent, FilterSearchInput
from usaspending_api.llm.tools.execute_filter import execute_filter_tool
from usaspending_api.llm.tools.lookup_agency import lookup_agency_tool
from usaspending_api.llm.tools.lookup_code import lookup_code_tool
from usaspending_api.llm.tools.lookup_location import lookup_location_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool
from usaspending_api.llm.v2.auth import LLMApiKeyAuth

logger = logging.getLogger(__name__)

router = Router(auth=LLMApiKeyAuth(), tags=["SmartAssist"])

TOOLS = [
    lookup_agency_tool,
    lookup_code_tool,
    lookup_location_tool,
    lookup_recipient_tool,
    execute_filter_tool,
]


def _ndjson(event: FilterSearchEvent) -> str:
    return event.model_dump_json(exclude_unset=True) + "\n"


def _stream_response(event_source: AsyncGenerator[str, None]) -> StreamingHttpResponse:
    response = StreamingHttpResponse(event_source, content_type="application/x-ndjson")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@router.post(
    "/filter-search/",
    url_name="filter_search",
)
async def filter_search(request: HttpRequest, payload: FilterSearchInput) -> StreamingHttpResponse:
    """
    Streaming, LLM-powered filter search. Emits newline-delimited FilterSearchEvent
    JSON objects (application/x-ndjson) as search progress, tool calls, and results
    become available.
    """
    query = payload.query

    try:
        assistant_config = await Assistant.objects.select_related("ai_model", "system_prompt").aget(
            name="filter-search", is_active=True
        )
    except Assistant.DoesNotExist:

        async def missing_assistant_stream() -> AsyncGenerator[str, Any]:
            yield _ndjson(FilterSearchEvent(type="search_error", message="Active filter-search Assistant not found."))

        return _stream_response(missing_assistant_stream())

    ai_model = assistant_config.ai_model
    if assistant_config.system_prompt is None:
        logger.warning("Active filter-search Assistant has no system prompt; using the default.")

    session = await Session.objects.acreate(
        ai_model=ai_model,
        tools=[tool.description.name for tool in TOOLS],
        system_prompt=assistant_config.system_prompt,
    )
    logger.info(
        f"Filter search session initialized: session_id={session.id}, model={ai_model.name}",
        extra={
            "session_id": str(session.id),
            "model_id": ai_model.model_id,
            "model_name": ai_model.name,
            "provider": ai_model.provider,
            "tools": [tool.description.name for tool in TOOLS],
            "query_length": len(query),
        },
    )

    assistant = FilterSearchAssistant(assistant=assistant_config, tools=TOOLS, session=session)

    async def event_stream() -> AsyncGenerator[str, Any]:
        try:
            async for event in assistant.search(query):
                yield _ndjson(FilterSearchEvent(**event))
        except Exception as e:
            logger.error(f"Error during filter search: {e}", exc_info=True)
            yield _ndjson(
                FilterSearchEvent(search_id=str(session.id), type="search_error", message="An error occurred.")
            )
        finally:
            session.ended_at = timezone.now()
            await session.asave(update_fields=["ended_at"])

            totals = await session.messages.aaggregate(
                input_tokens=Sum("input_tokens"), output_tokens=Sum("output_tokens")
            )
            message_count = await session.messages.acount()
            tool_use_count = await ToolUse.objects.filter(message__session=session).acount()
            duration_seconds = (session.ended_at - session.started_at).total_seconds()

            logger.info(
                f"Filter search session completed: session_id={session.id}, duration={duration_seconds:.3f}s",
                extra={
                    "session_id": str(session.id),
                    "duration_seconds": duration_seconds,
                    "message_count": message_count,
                    "tool_use_count": tool_use_count,
                    "total_tokens": (totals["input_tokens"] or 0) + (totals["output_tokens"] or 0),
                    "model_id": ai_model.model_id,
                },
            )

    return _stream_response(event_stream())
