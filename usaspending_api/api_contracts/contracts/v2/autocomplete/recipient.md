FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Autocomplete [/api/v2/autocomplete/recipient/]

This endpoint is used by the Recipient autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve recipients matching the specified search text.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `limit` (optional, number)
            + Default: 10
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Holdings"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[RecipientMatch], fixed-type)

# Data Structures

## RecipientMatch (object)
+ `recipient_name` (required, string)
+ `recipient_level` (required, array[string])
+ `uei` (optional, string)
