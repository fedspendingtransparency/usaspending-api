FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Account Count [/api/v2/agency/{toptier_code}/federal_account/count/{?fiscal_year}]

Returns the count of unique Federal Account and Treasury Account categories in the Agency's appropriations for a single fiscal year

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
            The desired appropriations fiscal year. Defaults to the current FY

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `federal_account_count` (required, number)
        + `treasury_account_count` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": 014,
                "fiscal_year": 2018,
                "federal_account_count": 7,
                "treasury_account_count": 7,
                "messages": ["Account data powering this endpoint were first collected in FY2017 Q2..."]
            }
