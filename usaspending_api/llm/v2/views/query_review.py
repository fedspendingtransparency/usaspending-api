import logging

from django.http import HttpRequest

from usaspending_api.common.helpers.decorators import browsable
from usaspending_api.llm.assistants.query_review import QueryReview
from usaspending_api.llm.models.db_models import Assistant, Session, SmartAssistSearch
from usaspending_api.llm.models.py_models import QueryReviewInput, QueryReviewOutput
from usaspending_api.llm.session_logging import finish_session
from usaspending_api.llm.v2.router import router

logger = logging.getLogger(__name__)


def _fail_open(search: SmartAssistSearch | None = None) -> QueryReviewOutput:
    """If this step cannot run, let the query go straight to filter search as typed."""
    return QueryReviewOutput(verdict="answerable", reason_code="ok", search_id=str(search.id) if search else None)


@browsable(
    router,
    "/query-review/",
    url_name="query_review",
    endpoint_doc="usaspending_api/api_contracts/contracts/v2/llm/query_review.md",
    response=QueryReviewOutput,
)
async def query_review(request: HttpRequest, payload: QueryReviewInput) -> QueryReviewOutput:
    """
    First step of Smart Assist. Decides whether the query can be turned into search filters, and may suggest a
    reworded query for the user to approve. If this step fails for any reason, it reports the query as answerable
    so that filter search can still run.
    """
    try:
        assistant_config = await Assistant.objects.select_related("ai_model", "system_prompt").aget(
            name="query-review", is_active=True
        )
    except Assistant.DoesNotExist:
        logger.warning("Active query-review Assistant not found; skipping the query review step.")
        return _fail_open()

    search = await SmartAssistSearch.objects.acreate(original_query=payload.query)
    session = await Session.objects.acreate(
        search=search,
        ai_model=assistant_config.ai_model,
        assistant=assistant_config,
        tools=[QueryReview.TOOL_NAME],
        system_prompt=assistant_config.system_prompt,
    )

    try:
        output = await QueryReview(assistant=assistant_config, session=session).evaluate(payload.query)
        logger.info(
            f"Query review decision: search_id={search.id}, verdict={output.verdict}, reason_code={output.reason_code}",
            extra={
                "search_id": str(search.id),
                "session_id": str(session.id),
                "model_id": assistant_config.ai_model.model_id,
                "verdict": output.verdict,
                "reason_code": output.reason_code,
                "query_length": len(payload.query),
            },
        )
        return output
    except Exception as e:
        logger.error(f"Error during query review: {e}", exc_info=True)
        return _fail_open(search)
    finally:
        await finish_session(session, assistant_config.ai_model, "Query review")
