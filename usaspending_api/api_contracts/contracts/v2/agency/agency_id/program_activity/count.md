FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activity Count [/api/v2/agency/{agency_id}/program_activity/count/{?fiscal_year}]

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
            The primary key of the agency record
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY

+ Response 200 (application/json)
    + Attributes
        + `program_activity_count` (required, number)

    + Body

            {
                "program_activity_count": 7
            }
