FORMAT: 1A
HOST: https://api.usaspending.gov

# List Program Activity [/api/v2/agency/{toptier_code}/program_activity/]

Returns a list of Program Activity in the Agency's appropriations for a single fiscal year

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 012 (required, number)
            The toptier code of an agency (could be a CGAG or FREC) so only numeric character strings of length 3-4 are accepted.
    + Attributes
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY.
        + `filter` (optional, string)
            This will filter the Program Activity by their name to those matching the text.
        + `order` (optional, enum[string])
            Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
            + Default: `desc`
            + Members
                + `desc`
                + `asc`
        + `page` (optional, number)
            The page number that is currently returned.
        + `limit` (optional, number)
            How many results are returned.
            + Default: 10
    + Body

            {
                "fiscal_year": 2018,
                "filter": "TRAINING",
                "order": "asc",
                "page": 3,
                "limit": 5
            }

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `results` (required, array[ProgramActivity], fixed-type)
        + `messages` (required, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "012",
                "fiscal_year": 2020,
                "program_activity_count": 7,
                "messages": ["Account data powering this endpoint were first collected in FY2017 Q2..."]
            }

# Data Structures

## ProgramActivity (object)
+ `code` (required, string)
+ `name` (required, string)
+ `obligated_amount` (required, number)
+ `percent_of_total_obligations` (required, number)
+ `gross_outlay_amount` (required, number)
