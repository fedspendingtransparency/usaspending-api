FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Autocomplete [/api/v2/autocomplete/recipient/]

## POST

This route sends a request to the backend to retrieve Parent and Recipient DUNS matching the search text in order of similarity.

+ Request (application/json)
    + Attributes (object)
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, object)
            + `recipient_id_list` (required, array[number])
            + `search_text` (required, string)
