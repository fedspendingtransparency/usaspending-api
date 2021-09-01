FORMAT: 1A
HOST: https://api.usaspending.gov

# Sub-Agency Count [/api/v2/agency/{toptier_code}/sub_agency/count/{?fiscal_year,agency_type}]

Returns the count of unique Sub-Agencies and Offices associated with an agency for a single fiscal year

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
        + `agency_type` (optional, enum[string])
            This will determine if the data being returned is derived from the awarding agency or the funding agency. Defaults to awarding
            + Default: `awarding`
            + Members
                + `awarding`
                + `funding`
+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `sub_agency_count` (required, number)
        + `office_count` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "012",
                "fiscal_year": 2018,
                "sub_agency_count": 20,
                "office_count": 32,
                "messages": []
            }