FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activity Count [/api/v2/agency/<agency_id>/program_activity/count/]

Returns the count of unique Program Activity categories in the Agency's appropriations for a single fiscal year

## GET


+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `agency_id`: 100 (required, number)

+ Response 200 (application/json)
    + Attributes
        + `count` (required, number)

    + Body

            {
                "count": 7
            }
