FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activity Count [/api/v2/agency/{toptier_code}/program_activity/count/{?fiscal_year}]

Returns the count of Program Activity categories in the Agency's appropriations for a single fiscal year

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 012 (required, number)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY.

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `program_activity_count` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "012",
                "fiscal_year": 2020,
                "program_activity_count": 7,
                "messages": ["Account data powering this endpoint were first collected in FY2017 Q2..."]
            }
