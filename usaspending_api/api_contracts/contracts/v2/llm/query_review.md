FORMAT: 1A
HOST: https://api.usaspending.gov

# LLM Query Review [UNDER DEVELOPMENT | NOT AVAILABLE] [/api/v2/llm/query-review/]

First step of Smart Assist. Decides whether a natural language query can be turned into search filters, and may return a reworded query for the user to approve. The client then calls `/api/v2/llm/filter-search/` with the original or approved query.

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
                + `out_of_scope` - Show `message`. Do not run filter search.
        + `message` (required, string)
            Plain language sentence for the user. For `answerable_after_rephrase` it gives the reason for the rewording; for `out_of_scope` it is an apology followed by the reason. Empty when the verdict is `answerable`.
        + `rewritten_query` (optional, string, nullable)
            Only present when the verdict is `answerable_after_rephrase`.
        + `search_id` (optional, string, nullable)
            UUID of the Smart Assist search. Send it as `search_id` in the filter search request so both steps belong to the same search. Absent when the query review was skipped.

    + Body

            {
                "verdict": "answerable_after_rephrase",
                "message": "Reworded your question as a search for matching awards.",
                "rewritten_query": "Awards to Arizona in 2025",
                "search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04"
            }
