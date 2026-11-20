import logging
from typing import Any

from usaspending_api.common.helpers.aws_helpers import (
    NO_BOTOCORE_RETRIES_CONFIG,
    async_aws_client,
    retry_on_throttling,
)
from usaspending_api.llm.assistants.filter_search import FilterSearchAssistant
from usaspending_api.llm.models.db_models import Assistant, Message, Session, SmartAssistSearch
from usaspending_api.llm.models.py_models import QueryReviewDecision, QueryReviewOutput

logger = logging.getLogger(__name__)

DEFAULT_OUT_OF_SCOPE_MESSAGE = "Smart Assist can't build a search for this question."


async def open_query_review(
    assistant_config: Assistant,
    query: str,
    *,
    search: SmartAssistSearch | None = None,
) -> "QueryReview":
    """
    Start a query review for ``query``: join or create its Smart Assist search and open the review's session.

    Shared by the query-review endpoint and the eval framework, so both run the same setup. The caller runs
    ``evaluate`` and then finishes the session (see ``session_logging.finish_session``).
    """
    if search is None:
        search = await SmartAssistSearch.objects.acreate(original_query=query)
    session = await Session.objects.acreate(
        search=search,
        ai_model=assistant_config.ai_model,
        assistant=assistant_config,
        tools=[QueryReview.TOOL_NAME],
        system_prompt=assistant_config.system_prompt,
    )
    return QueryReview(assistant=assistant_config, session=session)


class QueryReview:
    """
    First step of Smart Assist: decide whether a query can be turned into search filters, and optionally reword it.

    This is a single model call that submits a structured decision through one tool. It does not use the lookup tools.
    """

    TOOL_NAME = "submit_query_decision"
    DEFAULT_SYSTEM_MESSAGE = (
        "You decide whether a user's query can be turned into filters for searching USAspending award data."
    )

    def __init__(self, assistant: Assistant, session: Session) -> None:
        self.assistant = assistant
        self.session = session

    @property
    def system_message(self) -> str:
        if self.assistant.system_prompt:
            return self.assistant.system_prompt.text
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
                        "inputSchema": {"json": QueryReviewDecision.model_json_schema()},
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

    @property
    def search_id(self) -> str | None:
        """The id of the Smart Assist search this session belongs to, as returned to the frontend."""
        return str(self.session.search_id) if self.session.search_id else None

    @classmethod
    def _answerable(cls, search_id: str | None) -> QueryReviewOutput:
        return QueryReviewOutput(verdict="answerable", search_id=search_id)

    @classmethod
    def build_output(cls, decision: QueryReviewDecision, search_id: str | None) -> QueryReviewOutput:
        """Normalize the model's decision so the response is always safe for the frontend to act on."""
        if decision.verdict == "answerable_after_rephrase":
            rewritten = (decision.rewritten_query or "").strip()
            if not rewritten:
                # A rewrite verdict with nothing to approve is not actionable; let the search proceed as typed.
                return cls._answerable(search_id)
            return QueryReviewOutput(
                verdict="answerable_after_rephrase",
                message=decision.message,
                rewritten_query=rewritten,
                search_id=search_id,
            )
        if decision.verdict == "out_of_scope":
            return QueryReviewOutput(
                verdict="out_of_scope",
                message=decision.message.strip() or DEFAULT_OUT_OF_SCOPE_MESSAGE,
                search_id=search_id,
            )
        return cls._answerable(search_id)

    async def evaluate(self, query: str) -> QueryReviewOutput:
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
            logger.warning(f"Query review did not submit a decision: session={self.session.id}")
            return self._answerable(self.search_id)

        return self.build_output(QueryReviewDecision.model_validate(tool_input), self.search_id)
