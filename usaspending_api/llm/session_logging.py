import logging

from django.db.models import Q, Sum
from django.utils import timezone

from usaspending_api.llm.models.db_models import AIModel, Message, Session, SmartAssistSearch, ToolUse

logger = logging.getLogger(__name__)


async def _token_totals(*conditions: Q, **message_filters: object) -> int:
    totals = await Message.objects.filter(*conditions, **message_filters).aaggregate(
        input_tokens=Sum("input_tokens"), output_tokens=Sum("output_tokens")
    )
    return (totals["input_tokens"] or 0) + (totals["output_tokens"] or 0)


async def finish_session(session: Session, label: str) -> None:
    """Mark a session as ended and log its message, tool use and token totals.

    The model is the one the session recorded when it ran (`session.ai_model`), not the assistant's current model,
    which can be changed later.

    The query review and the filter search assistant run as separate sessions of one SmartAssistSearch. When the
    session belongs to a search, the search is marked as ended too (the last session to finish wins), and the token
    total across all of the search's sessions is logged, so the cost of a whole user search is visible in one place.
    """
    session.ended_at = timezone.now()
    await session.asave(update_fields=["ended_at"])

    message_count = await session.messages.acount()
    tool_use_count = await ToolUse.objects.filter(message__session=session).acount()
    duration_seconds = (session.ended_at - session.started_at).total_seconds()
    total_tokens = await _token_totals(session=session)
    model_id = (
        await AIModel.objects.filter(pk=session.ai_model_id).values_list("model_id", flat=True).afirst()
        if session.ai_model_id
        else None
    )

    extra = {
        "session_id": str(session.id),
        "search_id": str(session.search_id) if session.search_id else None,
        "duration_seconds": duration_seconds,
        "message_count": message_count,
        "tool_use_count": tool_use_count,
        "total_tokens": total_tokens,
        "model_id": model_id,
    }

    if session.search_id:
        await SmartAssistSearch.objects.filter(id=session.search_id).aupdate(ended_at=session.ended_at)
        extra["search_total_tokens"] = await _token_totals(session__search_id=session.search_id)

    logger.info(f"{label} session completed: session_id={session.id}, duration={duration_seconds:.3f}s", extra=extra)
