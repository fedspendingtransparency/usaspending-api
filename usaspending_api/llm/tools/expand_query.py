import boto3
from usaspending_api.llm.models.db_models import AIModel


def expand_query(query: str, num_variations: int = 3, model: AIModel = None) -> list[str]:
    """Generate related search queries using Amazon Bedrock converse API with tool"""
    if model is None:
        model = AIModel.objects.get(name="nova micro")
    try:
        client = boto3.client(service_name="bedrock-runtime")

        # Define the tool for query expansion
        tool_spec = {
            "toolSpec": {
                "name": "expand_search_query",
                "description": "Generates related search phrases to improve search recall",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "variations": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": f"List of {num_variations} related search phrases",
                            }
                        },
                        "required": ["variations"],
                    }
                },
            }
        }

        tool_config = {"tools": [tool_spec]}

        system = [
            {
                "text": f"""You are a search query expansion assistant. Given a search query, generate {num_variations} related search phrases that would help find similar items.

                Examples:
                - Query: "aerospace" → ["aircraft manufacturing", "space technology", "aviation industry"]
                - Query: "software development" → ["application programming", "IT services", "custom software"]
                - Query: "construction" → ["building contractors", "infrastructure development", "civil engineering"]

                Generate variations that are:
                1. Semantically related to the original query
                2. Use different terminology or synonyms
                3. Cover different aspects of the topic"""
            }
        ]

        messages = [
            {
                "role": "user",
                "content": [{"text": f"Generate {num_variations} related search phrases for: '{query}'"}],
            }
        ]
        model_id = model.model_id
        # First call to get tool use
        response = client.converse(modelId=model_id, messages=messages, toolConfig=tool_config, system=system)

        output_message = response["output"]["message"]
        stop_reason = response["stopReason"]
        if stop_reason == "tool_use":
            # Extract tool use from response
            tool_use = None
            for content in output_message["content"]:
                if "toolUse" in content:
                    tool_use = content["toolUse"]
                    break

            if tool_use and tool_use["name"] == "expand_search_query":
                variations = tool_use["input"].get("variations", [])

                if isinstance(variations, list) and len(variations) > 0:
                    print(f"  Generated variations: {variations}")
                    return [query] + variations[:num_variations]

        # If tool wasn't used or failed, return original query
        print(f"  Query expansion didn't use tool (stop_reason: {stop_reason}), using original query only")
        return [query]

    except Exception as e:
        print(f"  Query expansion failed: {e}")
        import traceback

        traceback.print_exc()
        return [query]  # Fallback to original only
