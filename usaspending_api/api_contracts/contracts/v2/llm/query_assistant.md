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
                + `out_of_scope` - Show `message`, `caveat` and `suggestions`. Do not run filter search.
        + `reason_code` (required, enum[string])
            + Members
                + `ok`
                + `no_matching_data`
                + `needs_comparison`
                + `needs_aggregation`
                + `ambiguous_time_period`
                + `not_a_search`
        + `message` (required, string)
            Plain language explanation for the user. Empty when the verdict is `answerable`.
        + `rewritten_query` (optional, string, nullable)
            Only present when the verdict is `answerable_after_rephrase`.
        + `caveat` (optional, string, nullable)
            What the user should know about the limits of the tool. Only present when the verdict is `out_of_scope`.
        + `suggestions` (required, array[string])
            Example prompts the user can click. For a comparison these are the individual searches to run one at a time; otherwise they are curated examples.
        + `suggestions_heading` (required, string)
            Label to show above `suggestions`.
        + `search_id` (optional, string, nullable)
            UUID of the session that handled this request.

    + Body

            {
                "verdict": "answerable_after_rephrase",
                "reason_code": "no_matching_data",
                "message": "Awards are not tagged by conflict, so I can search for related awards instead.",
                "rewritten_query": "Department of Defense awards with a place of performance in Iran",
                "caveat": "Awards are not tagged by conflict, so a total here is not the cost of the war.",
                "suggestions": [
                    "Department of Defense contracts awarded in fiscal year 2025",
                    "Grants to universities in Texas in fiscal year 2024",
                    "Department of Education spending in fiscal year 2024"
                ],
                "search_id": "a3e2c8f1-4b6d-4e9a-9c21-7f8b2d1e5a04"
            }
