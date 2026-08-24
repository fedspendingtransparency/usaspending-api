import logging
import time
from functools import cached_property
from typing import Any, Generator

import boto3

from usaspending_api.llm.models.db_models import AIModel, Message, Session, ToolUse
from usaspending_api.llm.models.py_models import AITool

logger = logging.getLogger(__name__)


class FilterSearchAssistant:
    MAX_TOOL_ITERATIONS = 15
    COMPLETION_TOOL_NAME = "execute_filter"

    def __init__(
        self,
        model: AIModel,
        tools: list[AITool],
        session: Session,
        system_message: str = (
            "You are USAspending search assistant. Help the user select filters to search for federal spending"
        ),
    ) -> None:
        self.model = model
        self.tools = tools
        self.tools_by_name = {tool.description.name: tool for tool in tools}
        self.session = session
        self.system_message = system_message

        self.message_order = 0
        self.messages = []

        self.tool_iterations = 0

    @cached_property
    def client(self) -> Any:
        """
        Lazy-load the Bedrock client so instantiation is deferred to first access and cached thereafter.
        This prevents the client from being created and never used (e.g., if __init__ fails).

        Returns:
            boto3 Bedrock Runtime client.
        """
        return boto3.client("bedrock-runtime")

    @staticmethod
    def _extract_text_from_content(content: list[dict]) -> str:
        """
        Safely extract text content from Bedrock message's "content" array.

        The "content" array can contain multiple block types (e.g., text, toolUse, image, etc.).
        This method finds and concatenates all text blocks, handling cases where:
        - No text block exists (e.g., tool-only response -> returns empty string);
        - Multiple text blocks exist (-> concatenates them together); and,
        - Text blocks are in any position in the array (not just content[0]) -> (collects/concatenates them).

        Args:
            content: List of content blocks from Bedrock response.

        Returns:
            Concatenated text from all text blocks, or an empty string if none are found.

        References:
            AWS Bedrock ContentBlock documentation:
            https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ContentBlock.html
        """
        text_blocks = [block.get("text", "") for block in content if "text" in block]
        return " ".join(text_blocks).strip()

    def _create_message_from_response(self, response: dict) -> Message:
        """Create a Message record from Bedrock's response."""
        output_message = response["output"]["message"]

        # Safely extract text content.
        message_text = self._extract_text_from_content(output_message)

        message = Message.objects.create(
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

    @cached_property
    def inference_config(self) -> dict:
        """
        Controls LLM response behavior.

        Uses model's inference_config if available, otherwise falls back to defaults.
        Defaults are optimized for deterministic responses.

        Returns:
            Dictionary with inference parameters (temperature, topP, maxTokens, stopSequences).
        """
        if self.model.inference_config:
            return self.model.inference_config

        # Default configuration for deterministic output.
        return {
            "temperature": 0.0,
            "topP": 1.0,
            "maxTokens": 5000,
            "stopSequences": [],
        }

    def search(self, query: str) -> Generator[dict[str, str], None, None]:
        yield {"search_id": str(self.session.id), "type": "search_start", "message": "Thinking..."}

        logger.info(
            f"Starting filter search: session={self.session.id}, query_length={len(query)}",
            extra={
                "session_id": self.session.id,
                "model_id": self.model.model_id,
                "query_length": len(query),
            },
        )

        Message.objects.create(session=self.session, role="user", message=query, order=self.message_order)
        self.message_order += 1
        self.messages.append({"role": "user", "content": [{"text": query}]})
        response = self.client.converse(
            modelId=self.model.model_id,
            messages=self.messages,
            toolConfig=self.tool_config,
            system=[{"text": self.system_message}],
            inferenceConfig=self.inference_config,
        )
        m = self._create_message_from_response(response)
        stop_reason = response["stopReason"]
        search_complete = False

        logger.info(
            f"Initial filter search response received: session={self.session.id}, stop_reason={stop_reason}",
            extra={
                "session_id": self.session.id,
                "model_id": self.model.model_id,
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
            tool_requests = [request for request in response["output"]["message"]["content"] if "toolUse" in request]

            for event in self.handle_tool_use(tool_requests, m):
                yield event
                if event.get("type") == "search_complete":
                    search_complete = True

            if search_complete:
                break

            response = self.client.converse(
                modelId=self.model.model_id,
                messages=self.messages,
                toolConfig=self.tool_config,
                system=[{"text": self.system_message}],
                inferenceConfig=self.inference_config,
            )
            m = self._create_message_from_response(response)
            stop_reason = response["stopReason"]

            logger.info(
                f"Filter search response received (iteration {self.tool_iterations}): "
                f"session={self.session.id}, stop_reason={stop_reason}",
                extra={
                    "session_id": self.session.id,
                    "model_id": self.model.model_id,
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

        # Calculate total token usage for this search.
        total_input_tokens = sum(msg.input_tokens for msg in self.session.messages.all())
        total_output_tokens = sum(msg.output_tokens for msg in self.session.messages.all())
        total_tokens = total_input_tokens + total_output_tokens

        # Log each search.
        logger.info(
            f"Search completed for session {self.session.id}",
            extra={
                "session_id": self.session.id,
                "tool_iterations": self.tool_iterations,
                "search_complete": search_complete,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "total_tokens": total_tokens,
            },
        )

    def handle_tool_use(self, tool_requests: list[dict], message: Message) -> Generator[dict, None, None]:
        tool_result_message = {"role": "user", "content": []}
        for tool_request in tool_requests:
            tool_use = tool_request["toolUse"]
            t = ToolUse.objects.create(name=tool_use["name"], tool_input=tool_use["input"], message=message, result="")
            tool = self.tools_by_name[tool_use["name"]]

            yield {
                "search_id": str(self.session.id),
                "type": "tool_start",
                "tool_use_id": t.id,
                "message": tool.logging(tool_use["input"]) + "\n",
            }

            tool_start_time = time.time()
            try:
                result = tool.function(**tool_use["input"])
                execution_time_ms = (time.time() - tool_start_time) * 1000
                t.result = result
                t.save()

                logger.info(
                    f"Filter search tool execution successful: tool={tool.description.name}, "
                    f"execution_time_ms={execution_time_ms:.3f}",
                    extra={
                        "tool_name": tool.description.name,
                        "execution_time_ms": execution_time_ms,
                        "session_id": self.session.id,
                        "has_error": "error" in result,
                        "tool_use_id": t.id,
                        "tool_input": t.tool_input,
                        "tool_result": t.result,
                    },
                )

                yield {"search_id": str(self.session.id), "type": "tool_complete", "tool_use_id": t.id}
                tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
                tool_result_message["content"].append({"toolResult": tool_result})
            except Exception as e:
                execution_time_ms = (time.time() - tool_start_time) * 1000
                error_result = {"error": str(e)}
                t.result = error_result
                t.save()

                logger.error(
                    f"Filter search tool execution failed: tool={tool.description.name}, "
                    f"execution_time_ms={execution_time_ms:.3f}, error={str(e)}",
                    extra={
                        "tool_name": tool.description.name,
                        "execution_time_ms": execution_time_ms,
                        "session_id": self.session.id,
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
                    "tool_use_id": t.id,
                    "message": f"Tool execution failed: {str(e)}",
                }
            if tool.description.name == self.COMPLETION_TOOL_NAME and "error" not in result:
                yield {"search_id": str(self.session.id), "type": "search_complete", "result": result["hash"]}
        self.messages.append(tool_result_message)
