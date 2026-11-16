import logging
from typing import Any, AsyncGenerator, Awaitable, Callable

from django.http import HttpRequest, StreamingHttpResponse

from usaspending_api.common.helpers.decorators import browsable
from usaspending_api.llm.assistants.filter_explain import ASSISTANT_NAME as FILTER_EXPLAIN
from usaspending_api.llm.assistants.filter_explain import FilterExplain
from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import Assistant, Session, SmartAssistSearch
from usaspending_api.llm.models.py_models import FilterSearchEvent, FilterSearchInput
from usaspending_api.llm.session_logging import finish_session
from usaspending_api.llm.tools.execute_filter import execute_filter_tool
from usaspending_api.llm.tools.list_recipient_types import list_recipient_types_tool
from usaspending_api.llm.tools.lookup_agency import lookup_agency_tool
from usaspending_api.llm.tools.lookup_award import lookup_award_tool
from usaspending_api.llm.tools.lookup_code import lookup_code_tool
from usaspending_api.llm.tools.lookup_location import lookup_location_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool
from usaspending_api.llm.v2.router import router

logger = logging.getLogger(__name__)

tools = [
    lookup_agency_tool,
    lookup_award_tool,
    lookup_code_tool,
    lookup_location_tool,
    lookup_recipient_tool,
    list_recipient_types_tool,
    execute_filter_tool,
]


async def _build_explainer(search: SmartAssistSearch) -> Callable[[str, dict], Awaitable[str | None]] | None:
    """The function that writes the "why these filters" note, or None (no note) if filter-explain is not set up."""
    explain_config = (
        await Assistant.objects.select_related("ai_model", "system_prompt")
        .filter(name=FILTER_EXPLAIN, is_active=True)
        .afirst()
    )
    if explain_config is None:
        logger.warning(f"Active {FILTER_EXPLAIN} Assistant not found; searches will not include an explanation.")
        return None
    return FilterExplain(explain_config, search).explain


def _ndjson(event: FilterSearchEvent) -> str:
    return event.model_dump_json(exclude_unset=True) + "\n"


def _stream_response(event_source: AsyncGenerator[str, None]) -> StreamingHttpResponse:
    response = StreamingHttpResponse(event_source, content_type="application/x-ndjson")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@browsable(
    router,
    "/filter-search/",
    url_name="filter_search",
    endpoint_doc="usaspending_api/api_contracts/contracts/v2/llm/filter_search.md",
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

    # Join the Smart Assist search the query review started, or start one if there is none (or it is unknown)
    search = await SmartAssistSearch.objects.filter(id=payload.search_id).afirst() if payload.search_id else None
    if payload.search_id and search is None:
        logger.warning(f"Unknown search_id {payload.search_id}; starting a new Smart Assist search.")
    if search is None:
        search = await SmartAssistSearch.objects.acreate(original_query=query)
    await SmartAssistSearch.objects.filter(id=search.id).aupdate(approved_query=query)

    session = await Session.objects.acreate(
        search=search,
        ai_model=ai_model,
        assistant=assistant_config,
        tools=[tool.description.name for tool in tools],
        system_prompt=assistant_config.system_prompt,
    )
    logger.info(
        f"Filter search session initialized: search_id={search.id}, session_id={session.id}, model={ai_model.name}",
        extra={
            "search_id": str(search.id),
            "session_id": str(session.id),
            "model_id": ai_model.model_id,
            "model_name": ai_model.name,
            "provider": ai_model.provider,
            "tools": [tool.description.name for tool in tools],
            "query_length": len(query),
        },
    )

    assistant = FilterSearchAssistant(
        assistant=assistant_config,
        tools=tools,
        session=session,
        check_results=True,
        search_id=str(search.id),
        explainer=await _build_explainer(search),
    )

    async def event_stream() -> AsyncGenerator[str, Any]:
        try:
            async for event in assistant.search(query):
                yield _ndjson(FilterSearchEvent(**event))
        except Exception as e:
            logger.error(f"Error during filter search: {e}", exc_info=True)
            yield _ndjson(
                FilterSearchEvent(
                    search_id=str(search.id),
                    type="search_error",
                    message="We are unable to provide an answer to your question at this time.",
                )
            )
        finally:
            await finish_session(session, ai_model, "Filter search")

    return _stream_response(event_stream())
