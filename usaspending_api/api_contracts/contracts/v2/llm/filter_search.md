FORMAT: 1A
HOST: https://api.usaspending.gov

# LLM Filter Search [/api/v2/llm/filter-search/]

This endpoint provides a streaming response for LLM-powered search operations with advanced filtering capabilities. The response is delivered as a series of JSON chunks, allowing real-time updates on search progress, tool execution, and results.

## POST

This endpoint accepts a natural language query and returns a streaming response with search results and execution details.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `query` (required, string)
            The natural language query to be processed by the LLM filter search.

    + Body

            {
                "query": "Show me all contracts greater than $3M in California for IT services in 2023"
            }

+ Response 200 (application/x-ndjson)
    
    The response is a stream of newline-delimited JSON objects. Each chunk represents a different stage of the search process.

    + Attributes
        + `search_id` (required, string)
            Unique identifier for this search operation.
        + `tool_use_id` (optional, string)
            Identifier for the specific tool being executed. Present only for tool-related events.
        + `type` (required, enum[string])
            The type of event in the streaming response.
            + Members
                + `search_start` - Indicates the search has begun
                + `search_error` - Indicates an error occurred during search
                + `search_complete` - Indicates the search has completed successfully
                + `tool_start` - Indicates a tool execution has started
                + `tool_complete` - Indicates a tool execution has completed
                + `tool_error` - Indicates an error occurred during tool execution
        + `message` (optional, string)
            Human-readable message describing the current event or status.
        + `result` (optional, object)
            Contains the hash of the filter search result. Present only with `search_complete` events.

    + Body

            {"search_id": "12345", "type": "search_start", "message": "Thinking..."}
            {"search_id": "12345", "tool_use_id": "12345", "type": "tool_start", "message": "Searching for location: California"}
            {"search_id": "12345", "tool_use_id": "12345", "type": "tool_complete"}
            {"search_id": "12345", "tool_use_id": "12346", "type": "tool_start", "message": "Applying filters based on contracts with an award amount greater than 3 million dollars in California for IT services in Fisacal Year 2023"}
            {"search_id": "12345", "tool_use_id": "12346", "type": "tool_complete"}
            {"search_id": "12345", "type": "search_complete", "message": "Showing results for contracts with an award amount greater than 3 million dollars in California for IT services in Fisacal Year 2023", "result": "16ebdca405791cb0f23d4c7120606fa1"}

# Data Structures

## StreamChunk (object)
Represents a single chunk in the streaming response.

+ `search_id` (required, string)
    Unique identifier for the search operation.
+ `tool_use_id` (optional, string)
    Unique identifier for a tool execution. Only present for tool-related events.
+ `type` (required, enum[string])
    + Members
        + `search_start`
        + `search_error`
        + `search_complete`
        + `tool_start`
        + `tool_error`
        + `tool_complete`
+ `message` (optional, string)
    Descriptive message about the current event.
+ `result` (optional, object)
    Contains results or output data when applicable.

## Error Handling

When an error occurs, the stream will include a chunk with `type` set to either `search_error` or `tool_error`:

```json
{
    "search_id": "12345",
    "type": "search_error",
    "message": "Failed to process query: Invalid filter syntax",
}
