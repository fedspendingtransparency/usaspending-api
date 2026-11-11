import logging

from django.http import HttpRequest
from django.utils import timezone

from usaspending_api.common.helpers.decorators import browsable
from usaspending_api.llm.assistants.query_assistant import QueryAssistant
from usaspending_api.llm.models.db_models import Assistant, Session
from usaspending_api.llm.models.py_models import QueryAssistantInput, QueryAssistantOutput
from usaspending_api.llm.v2.router import router

logger = logging.getLogger(__name__)


def _fail_open(session: Session | None = None) -> QueryAssistantOutput:
    """If this step cannot run, let the query go straight to filter search as typed."""
    return QueryAssistantOutput(verdict="answerable", reason_code="ok", search_id=str(session.id) if session else None)


@browsable(
    router,
    "/query-assistant/",
    url_name="query_assistant",
    endpoint_doc="usaspending_api/api_contracts/contracts/v2/llm/query_assistant.md",
    response=QueryAssistantOutput,
)
async def query_assistant(request: HttpRequest, payload: QueryAssistantInput) -> QueryAssistantOutput:
    """
    First step of Smart Assist. Decides whether the query can be turned into search filters, and may suggest a
    reworded query for the user to approve. If this step fails for any reason, it reports the query as answerable
    so that filter search can still run.
    """
    try:
        assistant_config = await Assistant.objects.select_related("ai_model", "system_prompt").aget(
            name="query-assistant", is_active=True
        )
    except Assistant.DoesNotExist:
        logger.warning("Active query-assistant Assistant not found; skipping the query assistant step.")
        return _fail_open()

    session = await Session.objects.acreate(
        ai_model=assistant_config.ai_model,
        tools=[QueryAssistant.TOOL_NAME],
        system_prompt=assistant_config.system_prompt,
    )

    try:
        output = await QueryAssistant(assistant=assistant_config, session=session).evaluate(payload.query)
        logger.info(
            f"Query assistant decision: session_id={session.id}, verdict={output.verdict}, "
            f"reason_code={output.reason_code}",
            extra={
                "session_id": str(session.id),
                "model_id": assistant_config.ai_model.model_id,
                "verdict": output.verdict,
                "reason_code": output.reason_code,
                "query_length": len(payload.query),
            },
        )
        return output
    except Exception as e:
        logger.error(f"Error during query assistant: {e}", exc_info=True)
        return _fail_open(session)
    finally:
        session.ended_at = timezone.now()
        await session.asave(update_fields=["ended_at"])
