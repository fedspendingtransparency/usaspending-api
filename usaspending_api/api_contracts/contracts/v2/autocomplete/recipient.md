FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Autocomplete [/api/v2/autocomplete/recipient/]

This endpoint is used by the Recipient autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve recipients matching the specified search text. The endpoint will search for `search_text` in the `recipient_name`, `uei` and `duns` fields. The `recipient_levels` value can also be included to further filter results, but is not required.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "number",
                        "description": "The number of results to return. Default is 10. Max is 500.",
                        "default": 10,
                        "maximum": 500
                    },
                    "search_text": {
                        "type": "string",
                        "description": "The search text to match against recipient names or uei."
                    },
                    "recipient_levels": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "An array of recipient levels to filter results. E.g., ['P', 'R']"
                    }
                },
                "required": ["search_text"]
            }

    + Attributes (object)
        + `limit` (optional, number)
        + `search_text` (required, string)
        + `recipient_levels` (optional, array[string])
    + Body

            {
                "search_text": "Holdings"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[RecipientMatch], fixed-type)
        + `messages` (required, array[string], fixed-type)
        An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "results": [
                    {
                        "recipient_name": "LOCKHEED MARTIN",
                        "recipient_level": "P",
                        "uei": "PUETYBNR91Z3"
                        "duns": "956973523"
                    },
                    {
                        "recipient_name": "LOCKHEED MARTIN",
                        "recipient_level": "C",
                        "uei": "PUETYBNR91Z3",
                        "duns": "956973523"
                    },
                    {
                        "recipient_name": "LOCKHEED MARTIN",
                        "recipient_level": "R",
                        "uei": "null",
                        "duns": "null"
                    }
                ]
            }

# Data Structures

## RecipientMatch (object)
+ `recipient_name` (required, string)
+ `recipient_level` (required, string)
+ `uei` (required, string)
+ `duns` (required, string)
