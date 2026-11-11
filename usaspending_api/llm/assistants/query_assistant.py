import logging
from datetime import date
from typing import Any

from usaspending_api.common.helpers.aws_helpers import (
    NO_BOTOCORE_RETRIES_CONFIG,
    async_aws_client,
    retry_on_throttling,
)
from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.assistants.query_assistant_suggestions import suggestions_for
from usaspending_api.llm.models.db_models import Assistant, Message, Session
from usaspending_api.llm.models.py_models import QueryAssistantDecision, QueryAssistantOutput

logger = logging.getLogger(__name__)

DEFAULT_OUT_OF_SCOPE_MESSAGE = "Smart Assist can't build a search for this question."
SUGGESTIONS_HEADING = "Examples of searches I can run:"
SEPARATE_QUERIES_HEADING = "Search them one at a time:"


class QueryAssistant:
    """
    First step of Smart Assist: decide whether a query can be turned into search filters, and optionally reword it.

    This is a single model call that submits a structured decision through one tool. It does not use the lookup tools.
    """

    TOOL_NAME = "submit_query_decision"
    DEFAULT_SYSTEM_MESSAGE = (
        "You decide whether a user's query can be turned into filters for searching USAspending award data."
    )

    def __init__(self, assistant: Assistant, session: Session, as_of: date | None = None) -> None:
        self.assistant = assistant
        self.session = session
        self.as_of = as_of

    @property
    def system_message(self) -> str:
        if self.assistant.system_prompt:
            return self.assistant.system_prompt.text + FilterSearchAssistant._fiscal_year_date_context(self.as_of)
        return self.DEFAULT_SYSTEM_MESSAGE

    @property
    def inference_config(self) -> dict:
        config = self.assistant.inference_config or {"temperature": 0.0, "maxTokens": 1000}
        return {key: value for key, value in config.items() if value is not None}

    @property
    def tool_config(self) -> dict[str, Any]:
        return {
            "tools": [
                {
                    "toolSpec": {
                        "name": self.TOOL_NAME,
                        "description": "Submit the decision about whether and how the query can be searched.",
                        "inputSchema": {"json": QueryAssistantDecision.model_json_schema()},
                    }
                }
            ],
            "toolChoice": {"auto": {}},
        }

    @retry_on_throttling
    async def _converse(self, client: Any, messages: list[dict]) -> dict:
        return await client.converse(
            modelId=self.assistant.ai_model.model_id,
            messages=messages,
            toolConfig=self.tool_config,
            system=[{"text": self.system_message}],
            inferenceConfig=self.inference_config,
        )

    @classmethod
    def _answerable(cls, session: Session) -> QueryAssistantOutput:
        return QueryAssistantOutput(verdict="answerable", reason_code="ok", search_id=str(session.id))

    @classmethod
    def build_output(cls, decision: QueryAssistantDecision, session: Session) -> QueryAssistantOutput:
        """Normalize the model's decision so the response is always safe for the frontend to act on."""
        verdict = decision.verdict
        if verdict != "answerable" and decision.reason_code == "needs_comparison":
            # One approved rewrite becomes one filter set, so a comparison can never be a single rewrite.
            verdict = "out_of_scope"

        output = cls._answerable(session)
        if verdict == "answerable_after_rephrase":
            # A rewrite verdict with nothing to approve is not actionable; let the search proceed as typed.
            output = cls._rewrite_output(decision, session) or output
        elif verdict == "out_of_scope":
            output = cls._out_of_scope_output(decision, session)
        return output

    @classmethod
    def _rewrite_output(cls, decision: QueryAssistantDecision, session: Session) -> QueryAssistantOutput | None:
        rewritten = (decision.rewritten_query or "").strip()
        if not rewritten:
            return None
        return QueryAssistantOutput(
            verdict="answerable_after_rephrase",
            reason_code=decision.reason_code,
            message=decision.message,
            rewritten_query=rewritten,
            search_id=str(session.id),
        )

    @classmethod
    def _out_of_scope_output(cls, decision: QueryAssistantDecision, session: Session) -> QueryAssistantOutput:
        suggestions = suggestions_for(decision.reason_code)
        heading = SUGGESTIONS_HEADING
        separate = list(dict.fromkeys(q.strip() for q in decision.separate_queries if q.strip()))
        if decision.reason_code == "needs_comparison" and len(separate) >= 2:
            suggestions, heading = separate[:3], SEPARATE_QUERIES_HEADING
        return QueryAssistantOutput(
            verdict="out_of_scope",
            reason_code=decision.reason_code,
            message=decision.message.strip() or DEFAULT_OUT_OF_SCOPE_MESSAGE,
            caveat=decision.caveat,
            suggestions=suggestions,
            suggestions_heading=heading,
            search_id=str(session.id),
        )

    async def evaluate(self, query: str) -> QueryAssistantOutput:
        """Return the decision for ``query``. Raises on Bedrock failures; the caller decides how to fail."""
        messages = [{"role": "user", "content": [{"text": query}]}]
        await Message.objects.acreate(session=self.session, role="user", message=query, order=0)

        async with async_aws_client("bedrock-runtime", config=NO_BOTOCORE_RETRIES_CONFIG) as client:
            response = await self._converse(client, messages)

        output_message = response["output"]["message"]
        await Message.objects.acreate(
            session=self.session,
            role=output_message["role"],
            message=FilterSearchAssistant._extract_text_from_content(output_message["content"]),
            order=1,
            input_tokens=response["usage"]["inputTokens"],
            output_tokens=response["usage"]["outputTokens"],
            latency=response["metrics"]["latencyMs"],
        )

        tool_input = next(
            (
                block["toolUse"]["input"]
                for block in output_message["content"]
                if "toolUse" in block and block["toolUse"]["name"] == self.TOOL_NAME
            ),
            None,
        )
        if tool_input is None:
            logger.warning(f"Query assistant did not submit a decision: session={self.session.id}")
            return self._answerable(self.session)

        return self.build_output(QueryAssistantDecision.model_validate(tool_input), self.session)
