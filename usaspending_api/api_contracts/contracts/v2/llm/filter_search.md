FORMAT: 1A
HOST: https://api.usaspending.gov

# LLM Filter Search [UNDER DEVELOPMENT | NOT AVAILABLE] [/api/v2/llm/filter-search/]

This endpoint provides a streaming response for LLM-powered search operations with advanced filtering capabilities. The response is delivered as a series of JSON chunks, allowing real-time updates on search progress, tool execution, and results.

Every request starts with a query review. The review decides whether the question can be turned into search filters, and runs automatically. No approval is needed from the user:

- **Approved:** the search runs on the query as written.
- **Approved with rephrase:** the search runs on a reworded query. The reworded query is reported in the `search_start` event.
- **Out of scope:** the search does not run. The stream ends with an `out_of_scope` event whose `message` explains why.

If the query review cannot run, the search continues with the query as written.

**Status:** Under Development, Not currently available

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
            The natural language query to be processed by the LLM filter search. 1 to 1000 characters.

    + Body

            {
                "query": "Show me all contracts greater than $3M in California for IT services in 2023"
            }

+ Response 200 (application/x-ndjson)

    The response is a stream of newline-delimited JSON objects. Each chunk represents a different stage of the search process.

    + Attributes
        + `search_id` (optional, string)
            UUID identifying the Smart Assist search. Present on every event except the `search_error` and `search_blocked` events returned when the request is stopped before a search is created (see Request Moderation).
        + `tool_use_id` (optional, string)
            Identifier for the specific step or tool being executed. Present only for `tool_start`, `tool_complete` and `tool_error` events. The query review uses this type of event, with its own `tool_use_id`.
        + `type` (required, enum[string])
            The type of event in the streaming response.
            + Members
                + `tool_start` - A step or tool has started. The first one is the query review ("Reviewing your search").
                + `tool_complete` - A step or tool has finished.
                + `tool_error` - A tool failed. The search continues.
                + `search_start` - The search has begun. `message` states the query being searched. `result` holds the original and searched queries.
                + `search_complete` - Terminal. The search completed. `result` is the final filter hash.
                + `search_error` - Terminal. An error occurred, or the service is unavailable.
                + `search_blocked` - Terminal. The request was rejected by content moderation. Nothing was searched.
                + `out_of_scope` - Terminal. The query review decided Smart Assist cannot build a search for this question. `message` explains why.
        + `message` (required, string)
            Human-readable message describing the current event or status.
        + `result` (optional, object | string)
            Extra data for the event. For `search_start`, an object with `original_query` and `query` (the query being searched, which differs from the original when it was rephrased). For `search_complete`, the filter hash. Null for all other events.

    + Body (query searched as written)

            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_start", "message": "Reviewing your search"}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_complete", "message": "Review complete."}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "type": "search_start", "message": "Searching for Show me all contracts greater than $3M in California for IT services in 2023", "result": {"original_query": "Show me all contracts greater than $3M in California for IT services in 2023", "query": "Show me all contracts greater than $3M in California for IT services in 2023"}}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "12345", "type": "tool_start", "message": "Searching for location: California"}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "12345", "type": "tool_complete", "message": "Success."}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "12346", "type": "tool_start", "message": "Applying filters based on contracts with an award amount greater than 3 million dollars in California for IT services in Fiscal Year 2023"}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "12346", "type": "tool_complete", "message": "Success."}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "type": "search_complete", "message": "Search complete.", "result": "16ebdca405791cb0f23d4c7120606fa1"}

    + Body (query rephrased by the review)

            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_start", "message": "Reviewing your search"}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_complete", "message": "Review complete."}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "type": "search_start", "message": "Searching for Department of Defense contracts", "result": {"original_query": "Pentagon contracts", "query": "Department of Defense contracts"}}
            …

    + Body (out of scope)

            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_start", "message": "Reviewing your search"}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "tool_use_id": "9b1d…", "type": "tool_complete", "message": "Review complete."}
            {"search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04", "type": "out_of_scope", "message": "Sorry, Smart Assist can't help with this question. Awards are not labeled by conflict, so the data doesn't show spending on the Iran war."}

## Event Order

1. If the request is stopped by moderation, the stream is a single `search_blocked` or `search_error` event. See Request Moderation.
2. `tool_start` ("Reviewing your search"), then `tool_complete`.
3. Either:
    - `search_start`, then any number of `tool_start` / `tool_complete` / `tool_error` events, then a terminal `search_complete` or `search_error`; or
    - a terminal `out_of_scope`. Nothing else follows.

Every stream ends with exactly one terminal event: `search_complete`, `search_error`, `search_blocked` or `out_of_scope`. Clients should treat an unrecognized `type` as non-terminal so that new event types can be added without breaking them.

## Request Moderation

Requests are checked by an Amazon Bedrock Guardrail before anything else runs, and no search is created for a request that fails the check. The endpoint fails closed, so unmoderated content is not processed or stored. These events have no `search_id`.

If the request is rejected by the Guardrail, the stream contains a single `search_blocked` event. The `message` is always the same and does not say what was matched.

        {"type": "search_blocked", "message": "Smart Assist can't answer that, please try a different search."}

If the Guardrail is unavailable or misconfigured, the stream contains a single `search_error` event.

        {"type": "search_error", "message": "Sorry an error occurred."}

# Data Structures

## StreamChunk (object)
Represents a single chunk in the streaming response.

+ `search_id` (optional, string)
    UUID identifying the Smart Assist search. Absent on moderation events.
+ `tool_use_id` (optional, string)
    Unique identifier for a step or tool execution. Only present for tool events.
+ `type` (required, enum[string])
    + Members
        + `tool_start`
        + `tool_complete`
        + `tool_error`
        + `search_start`
        + `search_complete`
        + `search_error`
        + `search_blocked`
        + `out_of_scope`
+ `message` (required, string)
    Descriptive message about the current event.
+ `result` (optional, object | string)
    Results or output data when applicable.

## Error Handling

When an error ends the search, the stream includes a chunk with `type` set to `search_error`. A failed tool is reported with `tool_error`, which is not terminal.

```json
{
    "search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04",
    "type": "search_error",
    "message": "An error has occurred."
}
```
