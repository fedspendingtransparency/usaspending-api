"""
Last step of Smart Assist: a short note on why the final filters were selected, shown at the bottom of the panel.

This is a single model call with no tools. It runs as its own session of the same SmartAssistSearch, with its own
prompt, model and inference settings (the `filter-explain` Assistant), so its tokens and latency are accounted for
separately from the filter search and it can be tuned and evaluated on its own.
"""

import json
import logging
from datetime import date
from typing import Any

from usaspending_api.common.helpers.aws_helpers import (
    NO_BOTOCORE_RETRIES_CONFIG,
    async_aws_client,
    retry_on_throttling,
)
from usaspending_api.llm.models.db_models import Assistant, Message, Session, SmartAssistSearch
from usaspending_api.llm.session_logging import finish_session
from usaspending_api.llm.tools.helpers.search_filters import to_search_filters

logger = logging.getLogger(__name__)

ASSISTANT_NAME = "filter-explain"
DEFAULT_SYSTEM_MESSAGE = (
    "You write a short note, for a Smart Assist search on USAspending.gov, explaining why the given search filters "
    "were selected."
)
DEFAULT_INFERENCE_CONFIG = {"temperature": 0.0, "maxTokens": 400}


def _long_date(day: date) -> str:
    return f"{day:%B} {day.day}, {day.year}"


def _describe_time_period(filters: dict) -> dict[str, Any] | None:
    """How the time period was selected, which the search format (plain start and end dates) cannot say.

    A fiscal year is its own filter and is not a date range, so the explanation must be able to tell them apart.
    """
    if filters.get("timePeriodType") == "dr":
        ranges = [f"{tp['start_date']} to {tp['end_date']}" for tp in filters.get("time_period", [])]
        return {"selected_as": "date range", "date_ranges": ranges} if ranges else None

    fiscal_years = {
        fy: f"{_long_date(date(int(fy) - 1, 10, 1))} to {_long_date(date(int(fy), 9, 30))}"
        for fy in filters.get("timePeriodFY", [])
    }
    return {"selected_as": "fiscal year", "fiscal_years": fiscal_years} if fiscal_years else None


def build_explanation_prompt(query: str, filter_request: dict) -> str:
    """The user turn: the search and the applied filters, in the search format (names and codes, no UI bookkeeping).

    The time period is described as it was selected (fiscal year or date range) instead of as converted dates.
    """
    filters = to_search_filters(filter_request)
    time_period = _describe_time_period(filter_request.get("filters", filter_request))
    if time_period:
        filters["time_period"] = time_period
    else:
        filters.pop("time_period", None)
    return f"User's search: {query}\n\nFilters applied:\n{json.dumps(filters, indent=2, default=str)}"


class FilterExplain:
    """Writes the explanation for a finished search. `explain` never raises: any problem returns None."""

    def __init__(self, assistant: Assistant, search: SmartAssistSearch | None = None) -> None:
        self.assistant = assistant
        self.search = search

    @property
    def system_message(self) -> str:
        if self.assistant.system_prompt:
            return self.assistant.system_prompt.text
        return DEFAULT_SYSTEM_MESSAGE

    @property
    def inference_config(self) -> dict[str, Any]:
        config = self.assistant.inference_config or DEFAULT_INFERENCE_CONFIG
        return {key: value for key, value in config.items() if value is not None}

    async def explain(self, query: str, filter_request: dict) -> str | None:
        """A short note on why these filters were selected for `query`, or None if it could not be written."""
        session = await Session.objects.acreate(
            search=self.search,
            ai_model=self.assistant.ai_model,
            assistant=self.assistant,
            tools=[],
            system_prompt=self.assistant.system_prompt,
        )
        try:
            return await self._write(session, query, filter_request)
        except Exception as e:
            logger.warning(f"Could not write the explanation of the selected filters: {e!r}", exc_info=True)
            return None
        finally:
            await finish_session(session, self.assistant.ai_model, "Filter explain")

    async def _write(self, session: Session, query: str, filter_request: dict) -> str | None:
        prompt = build_explanation_prompt(query, filter_request)
        await Message.objects.acreate(session=session, role="user", message=prompt, order=0)

        async with async_aws_client("bedrock-runtime", config=NO_BOTOCORE_RETRIES_CONFIG) as client:
            response = await retry_on_throttling(client.converse)(
                modelId=self.assistant.ai_model.model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                system=[{"text": self.system_message}],
                inferenceConfig=self.inference_config,
            )

        output_message = response["output"]["message"]
        text = " ".join(block["text"] for block in output_message["content"] if "text" in block).strip()
        await Message.objects.acreate(
            session=session,
            role=output_message["role"],
            message=text,
            order=1,
            input_tokens=response["usage"]["inputTokens"],
            output_tokens=response["usage"]["outputTokens"],
            latency=response["metrics"]["latencyMs"],
        )
        return text or None
