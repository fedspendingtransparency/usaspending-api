FORMAT: 1A
HOST: https://api.usaspending.gov

# PSC Autocomplete [/api/v2/autocomplete/program_activity/]

This endpoint is used by the Advanced Search page.

## POST

This route sends a request to the backend to retrieve program activities and their names based on a search string.
This may be the 4-character program activity code or a name string.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `limit` (optional, number) - Number of results to return (min: 1, max: 500)
          + Default: `10`
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Meat"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[ProgramActivityMatch], fixed-type)

# Data Structures

## ProgramActivityMatch (object)
+ `program_activity_code` (required, string)
+ `program_activity_name` (required, string)
