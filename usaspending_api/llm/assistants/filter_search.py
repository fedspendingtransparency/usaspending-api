import logging
import time
import uuid
from datetime import date
from functools import cached_property
from typing import Any, Callable, AsyncGenerator

from django.db.models import Sum

from usaspending_api.common.helpers.aws_helpers import async_aws_client
from usaspending_api.llm.models.db_models import Assistant, Message, Session, ToolUse
from usaspending_api.llm.models.py_models import AITool

logger = logging.getLogger(__name__)


class FilterSearchAssistant:
    MAX_TOOL_ITERATIONS = 15
    COMPLETION_TOOL_NAME = "execute_filter"
    DEFAULT_SYSTEM_MESSAGE = (
        "You are USAspending search assistant. Help the user select filters to search for federal spending"
    )

    def __init__(self, assistant: Assistant, tools: list[AITool], session: Session) -> None:
        self.assistant = assistant
        self.tools = tools
        self.tools_by_name = {tool.description.name: tool for tool in tools}
        self.session = session
        self.message_order = 0
        self.messages = []
        self.tool_iterations = 0

    @staticmethod
    def _extract_text_from_content(content: list[dict]) -> str:
        """
        Safely extract text content from Bedrock message's "content" array.

        The "content" array can contain multiple block types (e.g., text, toolUse, image, etc.).
        This method finds and concatenates all text blocks, handling cases where:
        - No text block exists (e.g., tool-only response -> returns empty string);
        - Multiple text blocks exist (-> concatenates them together); and,
        - Text blocks are in any position in the array (not just content[0]) -> (collects/concatenates them).
        """
        text_blocks = [block.get("text", "") for block in content if "text" in block]
        return " ".join(text_blocks).strip()

    @staticmethod
    def _rekey_empty_string_key(value: Any, derive_key: Callable[[dict], str | None]) -> Any:
        """Replace an empty-string dict key with one derived from its value.

        Bedrock's Converse API rejects empty-string object keys anywhere in `toolUse.input`,
        and it validates the full message history on every call -- so once a message with such
        a key is in `self.messages`, every subsequent `converse()` call fails, permanently
        blocking the conversation. The model occasionally emits dict fields keyed by "" instead
        of the documented key format; rebuild the key from the value's own fields when possible.
        """
        if not isinstance(value, dict) or "" not in value:
            return value

        rekeyed = dict(value)
        entry = rekeyed.pop("")
        new_key = derive_key(entry) if isinstance(entry, dict) else None
        rekeyed[new_key or f"unknown_{uuid.uuid4().hex}"] = entry
        return rekeyed

    @classmethod
    def _sanitize_tool_use_input(cls, content: list[dict]) -> None:
        """Fix empty-string dict keys in `toolUse.input` blocks, in place.

        This insures that the selected agency and selected location filters have the correct keys
        """
        for block in content:
            tool_use = block.get("toolUse")
            tool_input = tool_use.get("input") if tool_use else None
            if not isinstance(tool_input, dict):
                continue

            for field in ("selectedAwardingAgencies", "selectedFundingAgencies"):
                if field in tool_input:
                    tool_input[field] = cls._rekey_empty_string_key(
                        tool_input[field],
                        lambda agency: (
                            f"{agency.get('id')}_{agency.get('agencyType')}"
                            if agency.get("id") is not None and agency.get("agencyType")
                            else None
                        ),
                    )

            if "selectedLocations" in tool_input:
                tool_input["selectedLocations"] = cls._rekey_empty_string_key(
                    tool_input["selectedLocations"], lambda location: location.get("identifier")
                )

    async def _create_message_from_response(self, response: dict) -> Message:
        """Create a Message record from Bedrock's response."""
        output_message = response["output"]["message"]

        # Safely extract text content.
        message_text = self._extract_text_from_content(output_message)

        message = await Message.objects.acreate(
            session=self.session,
            role=output_message["role"],
            message=message_text,
            order=self.message_order,
            input_tokens=response["usage"]["inputTokens"],
            output_tokens=response["usage"]["outputTokens"],
            latency=response["metrics"]["latencyMs"],
        )
        self.message_order += 1
        self.messages.append(output_message)
        return message

    @cached_property
    def tool_config(self) -> dict[str, list[dict]]:
        specs = [tool.description.model_dump() for tool in self.tools]
        return {"tools": [{"toolSpec": {"inputSchema": {"json": spec.pop("input_schema")}, **spec}} for spec in specs]}

    @staticmethod
    def _fiscal_year_date_context() -> str:
        """Build the current-date/fiscal-year string appended to the system prompt."""
        today = date.today()
        current_fy = today.year + 1 if today.month >= 10 else today.year
        return (
            f"\nThe current date is {today.strftime('%m/%d/%Y')}. The current federal fiscal year is "
            f"FY{current_fy} (the federal fiscal year runs Oct 1 - Sep 30 and is named for the calendar "
            f"year it ends in, so FY{current_fy} runs 10/01/{current_fy - 1} - 09/30/{current_fy}). "
            f"Use FY{current_fy} directly as 'this fiscal year' and FY{current_fy - 1} as 'last fiscal "
            f"year' - do not recompute the fiscal year from the date yourself."
        )

    @cached_property
    def system_message(self) -> str:
        """Return the active Assistant's system prompt or the default prompt."""
        if self.assistant.system_prompt:
            return self.assistant.system_prompt.text + self._fiscal_year_date_context()
        return self.DEFAULT_SYSTEM_MESSAGE

    @cached_property
    def inference_config(self) -> dict:
        """
        Controls LLM response behavior.

        Uses the active Assistant's inference configuration when provided; otherwise falls back to defaults.
        Defaults are optimized for deterministic responses.

        Returns:
            Dictionary with inference parameters (temperature, topP, maxTokens, stopSequences).
        """
        if self.assistant.inference_config:
            return {key: value for key, value in self.assistant.inference_config.items() if value is not None}

        # Default configuration for deterministic output.
        return {
            "temperature": 0.0,
            "topP": 1.0,
            "maxTokens": 5000,
            "stopSequences": [],
        }

    async def search(self, query: str) -> AsyncGenerator[dict[str, str], None]:
        yield {"search_id": str(self.session.id), "type": "search_start", "message": "Thinking..."}

        logger.info(
            f"Starting filter search: session={self.session.id}, query_length={len(query)}",
            extra={
                "session_id": str(self.session.id),
                "model_id": self.assistant.ai_model.model_id,
                "query_length": len(query),
            },
        )

        await Message.objects.acreate(session=self.session, role="user", message=query, order=self.message_order)
        self.message_order += 1
        self.messages.append({"role": "user", "content": [{"text": query}]})

        async with async_aws_client("bedrock-runtime") as client:
            response = await client.converse(
                modelId=self.assistant.ai_model.model_id,
                messages=self.messages,
                toolConfig=self.tool_config,
                system=[{"text": self.system_message}],
                inferenceConfig=self.inference_config,
            )
            self._sanitize_tool_use_input(response["output"]["message"]["content"])
            m = await self._create_message_from_response(response)
            stop_reason = response["stopReason"]
            search_complete = False

            logger.info(
                f"Initial filter search response received: session={self.session.id}, stop_reason={stop_reason}",
                extra={
                    "session_id": str(self.session.id),
                    "model_id": self.assistant.ai_model.model_id,
                    "input_tokens": response["usage"]["inputTokens"],
                    "output_tokens": response["usage"]["outputTokens"],
                    "latency_ms": response["metrics"]["latencyMs"],
                    "stop_reason": stop_reason,
                    "iteration": 0,
                    "message_id": m.id,
                    "message_text": m.message,
                },
            )
            while stop_reason == "tool_use" and not search_complete and self.tool_iterations < self.MAX_TOOL_ITERATIONS:
                self.tool_iterations += 1
                tool_requests = [
                    request for request in response["output"]["message"]["content"] if "toolUse" in request
                ]

                async for event in self.handle_tool_use(tool_requests, m):
                    yield event
                    if event.get("type") == "search_complete":
                        search_complete = True

                if search_complete:
                    break

                response = await client.converse(
                    modelId=self.assistant.ai_model.model_id,
                    messages=self.messages,
                    toolConfig=self.tool_config,
                    system=[{"text": self.system_message}],
                    inferenceConfig=self.inference_config,
                )
                self._sanitize_tool_use_input(response["output"]["message"]["content"])
                m = await self._create_message_from_response(response)
                stop_reason = response["stopReason"]

                logger.info(
                    f"Filter search response received (iteration {self.tool_iterations}): "
                    f"session={self.session.id}, stop_reason={stop_reason}",
                    extra={
                        "session_id": str(self.session.id),
                        "model_id": self.assistant.ai_model.model_id,
                        "input_tokens": response["usage"]["inputTokens"],
                        "output_tokens": response["usage"]["outputTokens"],
                        "latency_ms": response["metrics"]["latencyMs"],
                        "stop_reason": stop_reason,
                        "iteration": self.tool_iterations,
                        "message_id": m.id,
                        "message_text": m.message,
                    },
                )

        # Communicate if tool iteration limit reached.
        if self.tool_iterations >= self.MAX_TOOL_ITERATIONS and not search_complete:
            yield {
                "search_id": str(self.session.id),
                "type": "search_error",
                "message": f"Maximum tool iterations ({self.MAX_TOOL_ITERATIONS}) reached without completing search.",
            }
        elif self.tool_iterations < self.MAX_TOOL_ITERATIONS and not search_complete:
            logger.info("Search not complete. Restarting search")
            self.search("Search is not complete.  You must call execute_filter to complete the search.")

        # Calculate total token usage for this search.
        totals = await self.session.messages.aaggregate(
            input_tokens=Sum("input_tokens"), output_tokens=Sum("output_tokens")
        )
        total_input_tokens = totals["input_tokens"] or 0
        total_output_tokens = totals["output_tokens"] or 0
        total_tokens = total_input_tokens + total_output_tokens

        # Log each search.
        logger.info(
            f"Search completed for session {self.session.id}",
            extra={
                "session_id": str(self.session.id),
                "tool_iterations": self.tool_iterations,
                "search_complete": search_complete,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "total_tokens": total_tokens,
            },
        )

    async def handle_tool_use(self, tool_requests: list[dict], message: Message) -> AsyncGenerator[dict, None]:
        tool_result_message = {"role": "user", "content": []}
        for tool_request in tool_requests:
            tool_use = tool_request["toolUse"]
            t = await ToolUse.objects.acreate(
                name=tool_use["name"], tool_input=tool_use["input"], message=message, result=""
            )
            tool = self.tools_by_name[tool_use["name"]]

            yield {
                "search_id": str(self.session.id),
                "type": "tool_start",
                "tool_use_id": str(t.id),
                "message": tool.logging(tool_use["input"]) + "\n",
            }

            tool_start_time = time.time()
            try:
                result = await tool.function(**tool_use["input"])
                execution_time_ms = (time.time() - tool_start_time) * 1000
                t.result = result
                await t.asave()

                logger.info(
                    f"Filter search tool execution successful: tool={tool.description.name}, "
                    f"execution_time_ms={execution_time_ms:.3f}",
                    extra={
                        "tool_name": tool.description.name,
                        "execution_time_ms": execution_time_ms,
                        "session_id": str(self.session.id),
                        "has_error": "error" in result,
                        "tool_use_id": t.id,
                        "tool_input": t.tool_input,
                        "tool_result": t.result,
                    },
                )

                yield {
                    "search_id": str(self.session.id),
                    "type": "tool_complete",
                    "tool_use_id": str(t.id),
                    "message": "Success.",
                }
                tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
                tool_result_message["content"].append({"toolResult": tool_result})
            except Exception as e:
                execution_time_ms = (time.time() - tool_start_time) * 1000
                error_result = {"error": str(e)}
                t.result = error_result
                await t.asave()

                logger.error(
                    f"Filter search tool execution failed: tool={tool.description.name}, "
                    f"execution_time_ms={execution_time_ms:.3f}, error={str(e)}",
                    extra={
                        "tool_name": tool.description.name,
                        "execution_time_ms": execution_time_ms,
                        "session_id": str(self.session.id),
                        "error": str(e),
                        "tool_use_id": t.id,
                        "tool_input": t.tool_input,
                        "tool_result": t.result,
                    },
                    exc_info=True,
                )

                yield {
                    "search_id": str(self.session.id),
                    "type": "tool_error",
                    "tool_use_id": str(t.id),
                    "message": "Tool execution failed.",
                }
            if tool.description.name == self.COMPLETION_TOOL_NAME and "error" not in result:
                yield {
                    "search_id": str(self.session.id),
                    "type": "search_complete",
                    "result": result["hash"],
                    "message": "Search complete.",
                }
        self.messages.append(tool_result_message)
