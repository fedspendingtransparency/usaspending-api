FORMAT: 1A
HOST: https://api.usaspending.gov

# LLM Query Assistant [UNDER DEVELOPMENT | NOT AVAILABLE] [/api/v2/llm/query-assistant/]

First step of Smart Assist. Decides whether a natural language query can be turned into search filters, and may suggest a reworded query for the user to approve. The client then calls `/api/v2/llm/filter-search/` with the original or approved query.

If this step fails for any reason, it responds with verdict `answerable` so that filter search can still run.

**Status:** Under Development, Not currently available

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `query` (required, string)
            The natural language query the user typed. 1 to 1000 characters.

    + Body

            {
                "query": "How much has been spent, as of today, on the Iran war?"
            }

+ Response 200 (application/json)

    + Attributes (object)
        + `verdict` (required, enum[string])
            + Members
                + `answerable` - Filters can be built from the query as written. Proceed to filter search.
                + `answerable_after_rephrase` - Show `rewritten_query` to the user with Approve and Edit.
                + `out_of_scope` - Show `message` and `suggestions`. Do not run filter search.
        + `reason_code` (required, enum[string])
            + Members
                + `ok`
                + `no_matching_data`
                + `needs_comparison`
                + `needs_aggregation`
                + `ambiguous_time_period`
                + `not_a_search`
                + `too_broad` - Nothing in the query can become a filter, for example "show me government spending".
        + `message` (required, string)
            Plain language sentence for the user. For `answerable_after_rephrase` it gives the reason for the rewording; for `out_of_scope` it is an apology followed by the reason. Empty when the verdict is `answerable`.
        + `rewritten_query` (optional, string, nullable)
            Only present when the verdict is `answerable_after_rephrase`.
        + `suggestions` (required, array[string])
            Example prompts the user can click. Only populated when the verdict is `out_of_scope`. For a comparison these are the individual searches to run one at a time; otherwise they are curated examples.
        + `suggestions_heading` (required, string)
            Label to show above `suggestions`.
        + `search_id` (optional, string, nullable)
            UUID of the session that handled this request.

    + Body

            {
                "verdict": "answerable_after_rephrase",
                "reason_code": "needs_aggregation",
                "message": "Reworded your question as a search for matching awards.",
                "rewritten_query": "Awards to Arizona in 2025",
                "suggestions": [],
                "suggestions_heading": "Examples of searches Smart Assist can run:",
                "search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04"
            }
