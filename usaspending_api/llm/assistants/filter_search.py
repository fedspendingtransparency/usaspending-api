import json
import logging
import time
import uuid
from datetime import date
from functools import cached_property
from typing import Any, AsyncGenerator, Awaitable, Callable

from django.db.models import Sum

from usaspending_api.common.helpers.aws_helpers import (
    NO_BOTOCORE_RETRIES_CONFIG,
    async_aws_client,
    retry_on_throttling,
)
from usaspending_api.llm.models.db_models import Assistant, Message, Session, SmartAssistSearch, ToolUse
from usaspending_api.llm.models.py_models import AITool, abuild_defc_description
from usaspending_api.llm.tools.exceptions import GENERIC_TOOL_FAILURE_INSTRUCTIONS, ToolUseRuntimeError
from usaspending_api.llm.tools.helpers.result_counts import get_award_counts, total_award_count
from usaspending_api.llm.tools.helpers.search_filters import to_search_filters
from usaspending_api.references.models import FilterHash

logger = logging.getLogger(__name__)


class FilterSearchAssistant:
    MAX_TOOL_ITERATIONS = 15
    MAX_RETRIES = 1
    COMPLETION_TOOL_NAME = "execute_filter"
    DEFAULT_SYSTEM_MESSAGE = (
        "You are USAspending search assistant. Help the user select filters to search for federal spending"
    )

    def __init__(  # noqa: PLR0913 - each option is a different post-search step; none can be dropped
        self,
        assistant: Assistant,
        tools: list[AITool],
        session: Session,
        as_of: date | None = None,
        check_results: bool = False,
        search_id: str | None = None,
        explainer: Callable[[str, dict], Awaitable[str | None]] | None = None,
    ) -> None:
        """
        `check_results` turns on the steps that run once `execute_filter` has saved the filters: counting the awards
        they return (and allowing one revision when there are none) and writing the "why these filters" note. It is off
        by default so evals and direct callers see the plain search; the API view turns it on.

        `explainer` writes the "why these filters" note from the query and the saved filter request (see
        FilterExplain.explain). It runs once the filters are final, and no note is sent when there is none.

        `search_id` is the id of the SmartAssistSearch this session belongs to. It is what the events report as their
        `search_id` (the session id is used when there is none), and the final hash and explanation are saved on it.
        """
        self.assistant = assistant
        self.as_of = as_of
        self.tools = tools
        self.tools_by_name = {tool.description.name: tool for tool in tools}
        self.session = session
        self._search_id = search_id
        self.message_order = 0
        self.messages = []
        self.tool_iterations = 0
        self.check_results = check_results
        self.explainer = explainer
        self.retries_used = 0
        self.search_complete = False
        # set by handle_tool_use when execute_filter succeeds and the result is still to be checked
        self.pending_completion: dict[str, str] | None = None
        # the last saved filters that matched nothing, kept in case the revision does not produce better ones
        self.fallback_completion: dict[str, str] | None = None
        self._tool_config: dict[str, list[dict]] | None = None

    @property
    def search_id(self) -> str:
        return self._search_id or str(self.session.id)

    async def _record_search(self, **fields: str) -> None:
        """Save results on the SmartAssistSearch. A no-op when this assistant was not given one (evals, tests)."""
        if self._search_id:
            await SmartAssistSearch.objects.filter(id=self._search_id).aupdate(**fields)

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
        message_text = self._extract_text_from_content(output_message["content"])

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

    async def aget_tool_config(self) -> dict[str, Any]:
        if self._tool_config is None:
            specs = [tool.description.model_dump() for tool in self.tools]
            for spec in specs:
                def_codes_property = spec["input_schema"].get("properties", {}).get("defCodes")
                if def_codes_property is not None:
                    def_codes_property["description"] = await abuild_defc_description()
            self._tool_config = {
                "tools": [{"toolSpec": {"inputSchema": {"json": spec.pop("input_schema")}, **spec}} for spec in specs]
            }

        return {**self._tool_config, "toolChoice": {"auto": {}}}

    @staticmethod
    def _fiscal_year_date_context(today: date | None = None) -> str:
        """Build the current-date/fiscal-year string appended to the system prompt (``today`` defaults to now)."""
        today = today or date.today()
        current_fy = today.year + 1 if today.month >= 10 else today.year
        return (
            f"\nThe current date is {today.strftime('%m/%d/%Y')}. The current federal fiscal year is "
            f"{current_fy} (the federal fiscal year runs Oct 1 - Sep 30 and is named for the calendar "
            f"year it ends in, so Fiscal Year {current_fy} runs 10/01/{current_fy - 1} - 09/30/{current_fy}). "
            f"Use {current_fy} directly as 'this fiscal year' and {current_fy - 1} as 'last fiscal "
            f"year' - do not recompute the fiscal year from the date yourself. When populating "
            f"timePeriodFY, use the bare four-digit year (e.g. '{current_fy}'), never the 'FY' prefix."
        )

    @property
    def system_message(self) -> str:
        """Return the active Assistant's system prompt or the default prompt."""
        if self.assistant.system_prompt:
            return self.assistant.system_prompt.text + self._fiscal_year_date_context(self.as_of)
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

    @retry_on_throttling
    async def _converse(self, client: Any) -> dict:
        """
        Call Bedrock's converse API, retrying with jittered backoff if AWS throttles the request.

        Retrying here (rather than re-running the whole search) means a transient error late in a conversation does not
        replay earlier iterations, which would consume more of the model's token quota.
        """
        return await client.converse(
            modelId=self.assistant.ai_model.model_id,
            messages=self.messages,
            toolConfig=await self.aget_tool_config(),
            system=[{"text": self.system_message}],
            inferenceConfig=self.inference_config,
        )

    async def search(self, query: str) -> AsyncGenerator[dict[str, str], None]:
        yield {"search_id": self.search_id, "type": "search_start", "message": "Thinking..."}

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

        async with async_aws_client("bedrock-runtime", config=NO_BOTOCORE_RETRIES_CONFIG) as client:
            response = await self._converse(client)
            self._sanitize_tool_use_input(response["output"]["message"]["content"])
            m = await self._create_message_from_response(response)
            stop_reason = response["stopReason"]

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
            while (
                stop_reason == "tool_use"
                and not self.search_complete
                and self.tool_iterations < self.MAX_TOOL_ITERATIONS
            ):
                self.tool_iterations += 1
                tool_requests = [
                    request for request in response["output"]["message"]["content"] if "toolUse" in request
                ]

                async for event in self._relay(self.handle_tool_use(tool_requests, m)):
                    yield event
                async for event in self._relay(self._finish_search(query)):
                    yield event

                if self.search_complete:
                    break

                response = await self._converse(client)
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

            if not self.search_complete:
                async for event in self._complete_after_loop(client, query, response, m):
                    yield event

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
                "search_complete": self.search_complete,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "total_tokens": total_tokens,
            },
        )

        if not self.search_complete:
            raise RuntimeError(f"Filter search session {self.session.id} ended without completing")

    async def _relay(self, events: AsyncGenerator[dict, None]) -> AsyncGenerator[dict, None]:
        """Pass events through, noting when the search has been reported complete."""
        async for event in events:
            if event.get("type") == "search_complete":
                self.search_complete = True
            yield event

    async def _complete_after_loop(
        self, client: Any, query: str, response: dict, message: Message
    ) -> AsyncGenerator[dict, None]:
        """Complete a search whose tool loop ended without a checked, completed execute_filter."""
        if self.fallback_completion:
            # the revision after a zero-result check did not produce new filters, so keep the ones we had
            self.pending_completion, self.fallback_completion = self.fallback_completion, None
            async for event in self._relay(self._finish_search(query)):
                yield event

        if not self.search_complete:
            async for event in self._relay(self._aforce_completion(client, query, response, message)):
                yield event
            async for event in self._relay(self._finish_search(query)):
                yield event

    def _search_complete_event(self, filter_hash: str) -> dict[str, str]:
        return {
            "search_id": self.search_id,
            "type": "search_complete",
            "result": filter_hash,
            "message": "Search complete.",
        }

    @staticmethod
    async def _saved_filter_request(filter_hash: str) -> dict | None:
        saved = await FilterHash.objects.filter(hash=filter_hash).afirst()
        return saved.filter if saved else None

    async def _add_user_turn_text(self, text: str) -> None:
        """Add text from the user to the conversation, merged into a trailing toolResult turn so roles alternate."""
        await Message.objects.acreate(session=self.session, role="user", message=text, order=self.message_order)
        self.message_order += 1
        if self.messages[-1]["role"] == "user":
            self.messages[-1]["content"].append({"text": text})
        else:
            self.messages.append({"role": "user", "content": [{"text": text}]})

    @staticmethod
    def _zero_results_note(filter_request: dict) -> str:
        filters = json.dumps(to_search_filters(filter_request), default=str)
        return (
            f"The filters were saved, but they match no awards. The filters were: {filters}\n"
            "Zero results can be the right answer. Check whether any filter is wrong or narrower than the user's "
            "search (for example a place or time period the user did not ask for, a code that is too specific, or a "
            "keyword phrase worded differently from how awards describe it; keywords are OR'd, so listing several "
            "common names for a topic widens the search). If so, call execute_filter again with corrected filters, "
            "using the lookup tools first if you need them. If the filters are right, reply with the single word OK "
            "and do not call any tool."
        )

    async def _finish_search(self, query: str) -> AsyncGenerator[dict, None]:
        """Check the awards the saved filters return. Ask for one revision if there are none, otherwise complete.

        A no-op unless execute_filter just succeeded with `check_results` on. The check is advisory: if the awards
        cannot be counted, the search simply completes.
        """
        completion, self.pending_completion = self.pending_completion, None
        if completion is None:
            return

        filter_request = await self._saved_filter_request(completion["hash"])
        counts = await get_award_counts(filter_request) if filter_request else None

        if counts is not None and total_award_count(counts) == 0 and self.retries_used < self.MAX_RETRIES:
            self.retries_used += 1
            self.fallback_completion = completion
            logger.info(f"Selected filters match no awards; asking for one revision: session={self.session.id}")
            await self._add_user_turn_text(self._zero_results_note(filter_request))
            yield {
                "search_id": self.search_id,
                "type": "search_retry",
                "message": "No awards matched these filters. Checking whether they can be improved.",
            }
            return

        self.fallback_completion = None
        await self._record_search(final_filter_hash=completion["hash"])
        yield self._search_complete_event(completion["hash"])

        explanation = await self._explain(query, filter_request) if filter_request else None
        if explanation:
            await self._record_search(explanation=explanation)
            yield {"search_id": self.search_id, "type": "search_explanation", "message": explanation}

    async def _explain(self, query: str, filter_request: dict) -> str | None:
        """The "why these filters" note, or None. Never fails the search: any problem returns None."""
        if self.explainer is None:
            return None
        try:
            return await self.explainer(query, filter_request) or None
        except Exception as e:
            logger.warning(f"Could not write the explanation of the selected filters: {e!r}", exc_info=True)
            return None

    async def _aforce_completion(
        self, client: Any, query: str, response: dict, message: Message
    ) -> AsyncGenerator[dict, None]:
        """Force the completion tool after the main loop ends without it."""
        if response["stopReason"] == "tool_use":
            pending = [request for request in response["output"]["message"]["content"] if "toolUse" in request]

            async for event in self.handle_tool_use(pending, message):
                yield event
                if event.get("type") == "search_complete":
                    return
            if self.pending_completion:
                # execute_filter succeeded in those pending calls; search() checks and completes it
                return

        logger.info(
            f"Search not complete after {self.tool_iterations} tool iterations. "
            f"Force calling {self.COMPLETION_TOOL_NAME}."
        )
        model_message = (
            f"Search is not complete and {self.COMPLETION_TOOL_NAME} was never called. As a last resort, "
            f'extract a single relevant keyword from the user\'s original query ("{query}") and call '
            f"{self.COMPLETION_TOOL_NAME} using that keyword."
        )

        await Message.objects.acreate(
            session=self.session, role="user", message=model_message, order=self.message_order
        )
        self.message_order += 1

        # Converse requires alternating roles, so merge into a trailing toolResult user turn.
        if self.messages[-1]["role"] == "user":
            self.messages[-1]["content"].append({"text": model_message})
        else:
            self.messages.append({"role": "user", "content": [{"text": model_message}]})

        response = await client.converse(
            modelId=self.assistant.ai_model.model_id,
            messages=self.messages,
            toolConfig={**await self.aget_tool_config(), "toolChoice": {"tool": {"name": self.COMPLETION_TOOL_NAME}}},
            system=[{"text": self.system_message}],
            inferenceConfig=self.inference_config,
        )
        self._sanitize_tool_use_input(response["output"]["message"]["content"])

        forced_message = await self._create_message_from_response(response)
        tool_requests = [request for request in response["output"]["message"]["content"] if "toolUse" in request]

        async for event in self.handle_tool_use(tool_requests, forced_message):
            yield event

    async def handle_tool_use(self, tool_requests: list[dict], message: Message) -> AsyncGenerator[dict, None]:
        tool_result_message = {"role": "user", "content": []}
        for tool_request in tool_requests:
            tool_use = tool_request["toolUse"]
            t = await ToolUse.objects.acreate(
                name=tool_use["name"], tool_input=tool_use["input"], message=message, result=""
            )
            tool = self.tools_by_name[tool_use["name"]]

            yield {
                "search_id": self.search_id,
                "type": "tool_start",
                "tool_use_id": str(t.id),
                "message": tool.logging(tool_use["input"]),
            }

            tool_start_time = time.time()
            try:
                tool_output = await tool.function(**tool_use["input"])
                execution_time_ms = (time.time() - tool_start_time) * 1000
                result = tool_output.model_dump(exclude_none=True)
                t.result = result
                await t.asave()

                logger.info(
                    f"Filter search tool execution successful: tool={tool.description.name}, "
                    f"execution_time_ms={execution_time_ms:.3f}",
                    extra={
                        "tool_name": tool.description.name,
                        "execution_time_ms": execution_time_ms,
                        "session_id": str(self.session.id),
                        "has_error": "error" in result.get("results", {}),
                        "tool_use_id": t.id,
                        "tool_input": t.tool_input,
                        "tool_result": t.result,
                    },
                )

                completion_event = None
                if tool.description.name == self.COMPLETION_TOOL_NAME:
                    if self.check_results:
                        # search() checks the saved filters before it reports the search as complete
                        self.pending_completion = {"hash": result["results"]["hash"]}
                    else:
                        completion_event = self._search_complete_event(result["results"]["hash"])

                yield {
                    "search_id": self.search_id,
                    "type": "tool_complete",
                    "tool_use_id": str(t.id),
                    "message": "Success.",
                }
                tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
                tool_result_message["content"].append({"toolResult": tool_result})

                if completion_event:
                    yield completion_event
            except Exception as e:
                execution_time_ms = (time.time() - tool_start_time) * 1000
                is_known_error = isinstance(e, ToolUseRuntimeError)
                error_message = e.user_message if is_known_error else "Tool execution failed."
                model_detail = e.model_detail if is_known_error else str(e)
                instructions = e.instructions if is_known_error else GENERIC_TOOL_FAILURE_INSTRUCTIONS
                error_result = {"error": model_detail}
                if instructions:
                    error_result["instructions"] = instructions
                t.result = error_result
                await t.asave()

                log = logger.warning if is_known_error else logger.error
                log(
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
                    exc_info=not is_known_error,
                )

                yield {
                    "search_id": self.search_id,
                    "type": "tool_error",
                    "tool_use_id": str(t.id),
                    "message": error_message,
                }
                tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": error_result}]}
                tool_result_message["content"].append({"toolResult": tool_result})
        self.messages.append(tool_result_message)
