import logging
from typing import Any, AsyncGenerator

from asgiref.sync import sync_to_async
from django.http import HttpRequest, StreamingHttpResponse

from usaspending_api.common.helpers.decorators import browsable
from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.assistants.query_review import QueryReview, open_query_review
from usaspending_api.llm.models.db_models import Assistant, Session, SmartAssistSearch
from usaspending_api.llm.models.py_models import FilterSearchEvent, FilterSearchInput, QueryReviewOutput
from usaspending_api.llm.services.guardrails import (
    BedrockGuardrailService,
    GuardrailConfigurationError,
    GuardrailServiceUnavailable,
)
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


def _ndjson(event: FilterSearchEvent) -> str:
    return event.model_dump_json(exclude_unset=True) + "\n"


def _stream_response(event_source: AsyncGenerator[str, None]) -> StreamingHttpResponse:
    response = StreamingHttpResponse(event_source, content_type="application/x-ndjson")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


BLOCKED_MESSAGE = "Smart Assist can't answer that, please try a different search."
ERROR_MESSAGE = "An error has occurred."
REVIEW_START_MESSAGE = "Reviewing your search"
REVIEW_COMPLETE_MESSAGE = "Review complete."


def _single_event_response(event_type: str, message: str) -> StreamingHttpResponse:
    async def event_stream() -> AsyncGenerator[str, Any]:
        yield _ndjson(FilterSearchEvent(type=event_type, message=message))

    return _stream_response(event_stream())


async def _run_query_through_guardrails(query: str) -> StreamingHttpResponse | None:
    """
    Run the query through Bedrock Guardrails to check for violations.

    Args:
        query: The query to check against Guardrails.

    Returns:
        StreamingHttpResponse | None: A single-event stream if the query is blocked or unchecked; otherwise None.
    """
    try:
        # Instantiate the service and assess the user query inside a thread.
        def _assess() -> Any:
            return BedrockGuardrailService().assess_input(query)

        guardrail_assessment = await sync_to_async(_assess)()
    # If Guardrails is misconfigured, return a search_error.
    except GuardrailConfigurationError:
        logger.exception("Filter-search request could not be moderated because Guardrails is misconfigured.")
        return _single_event_response("search_error", ERROR_MESSAGE)
    # If Guardrails is unavailable or unreachable (e.g., misconfigured variables or AWS issues), return a search_error.
    except GuardrailServiceUnavailable:
        logger.exception("Filter-search request could not be moderated because Guardrails is unavailable.")
        return _single_event_response("search_error", ERROR_MESSAGE)

    # If Guardrails intervenes, return a search_blocked.
    if guardrail_assessment.intervened:
        logger.warning(
            "Filter-search request rejected by Bedrock Guardrails.",
            extra={
                "guardrail_action_reason": guardrail_assessment.action_reason,
                "guardrail_assessments": guardrail_assessment.assessments,
            },
        )
        return _single_event_response("search_blocked", BLOCKED_MESSAGE)


async def _review_query(review: QueryReview, query: str) -> QueryReviewOutput | None:
    """Run the query review and end its session. Returns None if the review fails, so the search can go on."""
    try:
        output = await review.evaluate(query)
        logger.info(
            f"Query review decision: search_id={review.search_id}, verdict={output.verdict}",
            extra={
                "search_id": review.search_id,
                "session_id": str(review.session.id),
                "verdict": output.verdict,
                "query_length": len(query),
            },
        )
        return output
    except Exception as e:
        logger.error(f"Error during query review: {e}", exc_info=True)
        return None
    finally:
        await finish_session(review.session, "Query review")


@browsable(
    router,
    "/filter-search/",
    url_name="filter_search",
    endpoint_doc="usaspending_api/api_contracts/contracts/v2/llm/filter_search.md",
)
async def filter_search(request: HttpRequest, payload: FilterSearchInput) -> StreamingHttpResponse:
    """
    Streaming, LLM-powered filter search. Emits newline-delimited FilterSearchEvent JSON objects (application/x-ndjson)
    as search progress, tool calls, and results become available.

    The search starts with a query review, which either lets the query through as written, rewords it, or ends the
    stream with an out_of_scope event.
    """
    query = payload.query

    # Check query against Bedrock Guardrails.
    # This will prevent the stream from running if Guardrails intervenes; returns a search_blocked event instead.
    guardrail_response = await _run_query_through_guardrails(query)
    if guardrail_response is not None:
        return guardrail_response

    try:
        assistant_config = await Assistant.objects.select_related("ai_model", "system_prompt").aget(
            name="filter-search", is_active=True
        )
    except Assistant.DoesNotExist:
        return _single_event_response("search_error", ERROR_MESSAGE)

    ai_model = assistant_config.ai_model
    if assistant_config.system_prompt is None:
        logger.warning("Active filter-search Assistant has no system prompt; using the default.")

    review_config = (
        await Assistant.objects.select_related("ai_model", "system_prompt")
        .filter(name="query-review", is_active=True)
        .afirst()
    )
    if review_config is None:
        logger.warning("Active query-review Assistant not found; skipping the query review step.")

    search = await SmartAssistSearch.objects.acreate(original_query=query)
    search_id = str(search.id)

    async def event_stream() -> AsyncGenerator[str, Any]:
        session = None
        try:
            searched_query = query
            if review_config is not None:
                review = await open_query_review(review_config, query, search=search)
                review_id = str(review.session.id)
                yield _ndjson(
                    FilterSearchEvent(
                        search_id=search_id, tool_use_id=review_id, type="tool_start", message=REVIEW_START_MESSAGE
                    )
                )
                output = await _review_query(review, query)
                yield _ndjson(
                    FilterSearchEvent(
                        search_id=search_id,
                        tool_use_id=review_id,
                        type="tool_complete",
                        message=REVIEW_COMPLETE_MESSAGE,
                    )
                )
                if output is not None and output.verdict == "out_of_scope":
                    yield _ndjson(FilterSearchEvent(search_id=search_id, type="out_of_scope", message=output.message))
                    return
                if output is not None and output.rewritten_query:
                    searched_query = output.rewritten_query

            await SmartAssistSearch.objects.filter(id=search.id).aupdate(approved_query=searched_query)
            session = await Session.objects.acreate(
                search=search,
                ai_model=ai_model,
                assistant=assistant_config,
                tools=[tool.description.name for tool in tools],
                system_prompt=assistant_config.system_prompt,
            )
            logger.info(
                f"Filter search session initialized: search_id={search_id}, session_id={session.id}, "
                f"model={ai_model.name}",
                extra={
                    "search_id": search_id,
                    "session_id": str(session.id),
                    "model_id": ai_model.model_id,
                    "model_name": ai_model.name,
                    "provider": ai_model.provider,
                    "tools": [tool.description.name for tool in tools],
                    "query_length": len(searched_query),
                },
            )
            yield _ndjson(
                FilterSearchEvent(
                    search_id=search_id,
                    type="search_start",
                    message=f"Searching for {searched_query}",
                    result={"original_query": query, "query": searched_query},
                )
            )

            assistant = FilterSearchAssistant(
                assistant=assistant_config, tools=tools, session=session, search_id=search_id
            )
            async for event in assistant.search(searched_query):
                yield _ndjson(FilterSearchEvent(**event))
        except Exception as e:
            logger.error(f"Error during filter search: {e}", exc_info=True)
            yield _ndjson(
                FilterSearchEvent(
                    search_id=search_id,
                    type="search_error",
                    message=ERROR_MESSAGE,
                )
            )
        finally:
            if session is not None:
                await finish_session(session, "Filter search")

    return _stream_response(event_stream())
