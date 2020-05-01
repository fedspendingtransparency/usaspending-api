FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activity Count [/api/v2/agency/{toptier_code}/object_class/count/{?fiscal_year}]

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
            The toptier code of an agency (could be a CGAG or FREC) so only numeric character strings of length 3-4 are accepted.
        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY.

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `object_class_count` (required, number)

    + Body

            {
                "toptier_code": "012",
                "fiscal_year": 2018,
                "object_class_count": 81
            }
