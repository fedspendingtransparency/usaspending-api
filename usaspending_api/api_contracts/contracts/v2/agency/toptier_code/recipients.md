FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipients [/api/v2/agency/{toptier_code}/recipients/{?fiscal_year}]

Returns a list of data points of an agencies recipients for a gvien fiscal year

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 086 (required, number)
            The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY.

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `count` (required, number)
        + `max` (required, number)
        + `min` (required, number)
        + `25th_percentile` (required, number)
        + `50th_percentile` (required, number)
        + `75th_percentile` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "whatever": {}
            }

# Data Structures
