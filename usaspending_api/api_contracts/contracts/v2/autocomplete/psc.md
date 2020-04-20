FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC Autocomplete [/api/v2/autocomplete/psc/]

This endpoint is used by the Advanced Search page.

## POST

This route sends a request to the backend to retrieve product or service (PSC) codes and their descriptions based on a search string. This may be the 4-character PSC code or a description string.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `limit` (optional, number)
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[PSCMatch], fixed-type)

# Data Structures

## PSCMatch (object)
+ `product_or_service_code` (required, string)
+ `psc_description` (required, string)
