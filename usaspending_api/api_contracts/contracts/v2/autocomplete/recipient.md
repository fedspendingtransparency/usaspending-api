FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Autocomplete [/api/v2/autocomplete/recipient/]

This endpoint is used by the Recipient autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve recipients matching the specified search text. The endpoint will search for `search_text` in the `recipient_name` and `uei` fields. The `recipient_levels` value can also be included to further filter results, but is not required.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `limit` (optional, number)
            + Default: 10
            + Max: 500
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
                        "recipient_name": "ABC Holdings Inc.",
                        "recipient_level": "P",
                        "uei": "ABCDEF12345"
                    },
                    {
                        "recipient_name": "ABC Holdings Inc.",
                        "recipient_level": "R",
                        "uei": "ABCDEF12345"
                    },
                    {
                        "recipient_name": "XYZ Holdings",
                        "recipient_level": "C",
                        "uei": "ASDFGH67890"
                    }
                ]
            }

# Data Structures

## RecipientMatch (object)
+ `recipient_name` (required, string)
+ `recipient_level` (required, string)
+ `uei` (optional, string)
