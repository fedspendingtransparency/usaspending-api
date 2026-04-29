from typing import Any, Generator

import boto3

from usaspending_api.llm.models.db_models import AIModel, Message, Session, ToolUse
from usaspending_api.llm.models.py_models import AITool


class SearchAssistant:

    def __init__(
        self,
        model: AIModel,
        tools: list[AITool],
        session: Session,
        system_message: str = "You are USASpending search assistant. Help the user search for federal spending",
    ) -> None:
        self.model = model
        self.tools = tools
        self.session = session
        self.client = boto3.client("bedrock-runtime")
        self.system_message = system_message

    # TODO add support for anthropic search and break up and abstract the search functions
    def _amazon_search(self, query: str) -> Generator[dict[str, str], None, None]:

        # Create user message
        message_order = 0
        Message.objects.create(session=self.session, role="user", message=query, order=message_order)
        message_order += 1

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
        m = Message.objects.create(
            session=self.session,
            role=output_message["role"],
            message=output_message["content"][0]["text"],
            order=message_order,
            input_tokens=response["usage"]["inputTokens"],
            output_tokens=response["usage"]["outputTokens"],
            latency=response["metrics"]["latencyMs"],
        )
        message_order += 1
        messages.append(output_message)
        stop_reason = response["stopReason"]
        while stop_reason == "tool_use":
            tool_use = output_message["content"][1]["toolUse"]
            t = ToolUse.objects.create(name=tool_use["name"], input=tool_use["input"], message=m, result="")
            tool_result_message = {"role": "user", "content": []}
            tool_requests = response["output"]["message"]["content"]
            for tool_request in tool_requests[::-1]:
                if "toolUse" in tool_request:
                    tool_use = tool_request["toolUse"]
                    tool = [tool for tool in self.tools if tool.description.name == tool_use["name"]][0]

                    yield {"type": "tool", "message": tool.logging(tool_use["input"]) + "\n"}

                    result = tool.function(**tool_use["input"])
                    print(result)
                    t.result = result
                    t.save()
                    tool_result = {"toolUseId": tool_use["toolUseId"], "content": [{"json": result}]}
                    tool_result_message["content"].append({"toolResult": tool_result})
                    if tool.description.name == "search_federal_contracts_and_assistance" and "error" not in result:
                        yield {"type": "hash", "result": result["hash"]}
                        break

            messages.append(tool_result_message)
            response = self.client.converse(
                modelId=self.model.model_id, messages=messages, toolConfig=tool_config, system=system
            )
            output_message = response["output"]["message"]
            m = Message.objects.create(
                session=self.session,
                role=output_message["role"],
                message=output_message["content"][0]["text"],
                order=message_order,
                input_tokens=response["usage"]["inputTokens"],
                output_tokens=response["usage"]["outputTokens"],
                latency=response["metrics"]["latencyMs"],
            )
            message_order += 1
            messages.append(output_message)
            stop_reason = response["stopReason"]

    def search(self, query: str) -> Any:
        return self._amazon_search(query)
