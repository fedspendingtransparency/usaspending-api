from functools import cached_property
from typing import Generator

import boto3

from usaspending_api.llm.models.db_models import AIModel, Message, Session, ToolUse
from usaspending_api.llm.models.py_models import AITool


class FilterSearchAssistant:

    MAX_TOOL_ITERATIONS = 15

    def __init__(
        self,
        model: AIModel,
        tools: list[AITool],
        session: Session,
        system_message: str = (
            "You are USASpending search assistant. " "Help the user select filters to search for federal spending"
        ),
    ) -> None:
        self.model = model
        self.tools = tools
        self.tools_by_name = {tool.description.name: tool for tool in tools}
        self.session = session
        self.client = boto3.client("bedrock-runtime")
        self.system_message = system_message

        self.message_order = 0
        self.messages = []

        self.tool_iterations = 0

    @cached_property
    def tool_config(self) -> dict[str, list[dict]]:
        specs = [tool.description.model_dump() for tool in self.tools]
        return {"tools": [{"toolSpec": {"inputSchema": {"json": spec.pop("input_schema")}, **spec}} for spec in specs]}

    def search(self, query: str) -> Generator[dict[str, str], None, None]:

        yield {"search_id": self.session.id, "type": "search_start", "message": "Thinking..."}

        Message.objects.create(session=self.session, role="user", message=query, order=self.message_order)
        self.message_order += 1
        self.messages.append([{"role": "user", "content": [{"text": query}]}])
        response = self.client.converse(
            modelId=self.model.model_id,
            messages=self.messages,
            toolConfig=self.tool_config,
            system=[{"text": self.system_message}],
        )
        output_message = response["output"]["message"]
        self.messages.append(output_message)
        m = Message.objects.create(
            session=self.session,
            role=output_message["role"],
            message=output_message["content"][0]["text"],
            order=self.message_order,
            input_tokens=response["usage"]["inputTokens"],
            output_tokens=response["usage"]["outputTokens"],
            latency=response["metrics"]["latencyMs"],
        )
        self.message_order += 1
        stop_reason = response["stopReason"]
        search_complete = False
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
            )
            output_message = response["output"]["message"]
            m = Message.objects.create(
                session=self.session,
                role=output_message["role"],
                message=output_message["content"][0]["text"],
                order=self.message_order,
                input_tokens=response["usage"]["inputTokens"],
                output_tokens=response["usage"]["outputTokens"],
                latency=response["metrics"]["latencyMs"],
            )
            self.message_order += 1
            self.messages.append(output_message)
            stop_reason = response["stopReason"]

    def handle_tool_use(self, tool_requests: list[dict], message: Message) -> Generator[dict, None, None]:
        tool_result_message = {"role": "user", "content": []}
        for tool_request in tool_requests[::-1]:
            tool_use = tool_request["toolUse"]
            t = ToolUse.objects.create(name=tool_use["name"], tool_input=tool_use["input"], message=message, result="")
            tool = self.tools_by_name[tool_use["name"]]

            yield {
                "search_id": self.session.id,
                "type": "tool_start",
                "tool_use_id": t.id,
                "message": tool.logging(tool_use["input"]) + "\n",
            }

            result = tool.function(**tool_use["input"])
            t.result = result
            t.save()

            yield {"search_id": self.session.id, "type": "tool_complete", "tool_use_id": t.id}

            tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
            tool_result_message["content"].append({"toolResult": tool_result})
            if tool.description.name == "execute_filter" and "error" not in result:
                yield {"search_id": self.session.id, "type": "search_complete", "result": result["hash"]}
        self.messages.append(tool_result_message)
