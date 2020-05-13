FORMAT: 1A
HOST: https://api.usaspending.gov

# Object Class Count [/api/v2/agency/{toptier_code}/object_class/count/{?fiscal_year}]

Returns the count of Object Classes in the Agency's appropriations for a single fiscal year

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
        + `object_class_count` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "012",
                "fiscal_year": 2018,
                "object_class_count": 81,
                "messages": ["Account data powering this endpoint were first collected in FY2017 Q2..."]
            }
