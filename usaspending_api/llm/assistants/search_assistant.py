import json
from typing import Any, Generator

import boto3
from anthropic import AnthropicBedrock

from usaspending_api.llm.models.db_models import AIModel
from usaspending_api.llm.models.py_models import AITool


class SearchAssistant:

    def __init__(
        self,
        model: AIModel,
        tools: list[AITool],
        system_message: str = "You are USASpending search assistant. Help the user search for federal spending",
    ) -> None:
        self.model: AIModel = model
        self.tools = tools
        self.client = AnthropicBedrock() if self.model.provider == "anthropic" else boto3.client("bedrock-runtime")
        self.system_message = system_message

    def _amazon_search(self, query: str) -> Generator[dict[str, str], None, None]:
        specs = [tool.description.model_dump() for tool in self.tools]
        tool_config = {
            "tools": [{"toolSpec": {"inputSchema": {"json": spec.pop("input_schema")}, **spec}} for spec in specs]
        }
        system = [{"text": self.system_message}]
        messages = [{"role": "user", "content": [{"text": query}]}]
        response = self.client.converse(
            modelId=self.model.model_id, messages=messages, toolConfig=tool_config, system=system
        )
        output_message = response["output"]["message"]
        messages.append(output_message)
        stop_reason = response["stopReason"]
        while stop_reason == "tool_use":
            tool_result_message = {"role": "user", "content": []}
            tool_requests = response["output"]["message"]["content"]
            for tool_request in tool_requests[::-1]:
                if "toolUse" in tool_request:
                    tool_use = tool_request["toolUse"]
                    tool = [tool for tool in self.tools if tool.description.name == tool_use["name"]][0]

                    yield {"type": "tool", "message": tool.logging(tool_use["input"])}

                    result = tool.function(**tool_use["input"])
                    tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
                    tool_result_message["content"].append({"toolResult": tool_result})

                    yield {"type": "tool_result", "message": result}
            messages.append(tool_result_message)
            response = self.client.converse(
                modelId=self.model.model_id, messages=messages, toolConfig=tool_config, system=system
            )
            output_message = response["output"]["message"]
            messages.append(output_message)
            stop_reason = response["stopReason"]

    def _anthropic_search(self, query: str) -> Any:
        messages = [{"role": "user", "content": query}]
        response = self.client.messages.create(
            model=self.model.model_id,
            max_tokens=1024,
            tools=[tool.description for tool in self.tools],
            tool_choice={"type": "auto", "disable_parallel_tool_use": True},
            messages=messages,
            system=self.system_message,
        )
        while response.stop_reason == "tool_use":
            tool_use = next(block for block in response.content if block.type == "tool_use")
            tool = [tool for tool in self.tools if tool.description.name == tool_use.name][0]
            tool.logging(tool_use.input)
            result = tool.function(**tool_use.input)
            messages.append({"role": "assistant", "content": response.content})
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use.id,
                            "content": json.dumps(result),
                        }
                    ],
                }
            )
            response = self.client.messages.create(
                model=self.model.model_id,
                max_tokens=1024,
                tools=[tool.description for tool in self.tools],
                tool_choice={"type": "auto", "disable_parallel_tool_use": True},
                messages=messages,
                system=self.system_message,
            )
        return messages

    def search(self, query: str) -> Any:
        return self._anthropic_search(query) if self.model.provider == "anthropic" else self._amazon_search(query)
