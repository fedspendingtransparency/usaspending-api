FORMAT: 1A
HOST: https://api.usaspending.gov

# NAICS Autocomplete [/api/v2/autocomplete/naics/]

This endpoint is used by the Advanced Search page.

## POST

This route sends a request to the backend to retrieve NAICS objects matching the specified search text.

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
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[NAICSMatch], fixed-type)

# Data Structures

## NAICSMatch (object)
+ `naics` (required, string)
+ `naics_description` (required, string)
