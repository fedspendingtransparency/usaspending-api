FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/agency/{toptier_code}/budgetary_resources/{?fiscal_year}]

Returns budgetary resources for the agency and fiscal year indicated.

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
            The desired "as of" fiscal year. Defaults to the current fiscal year.

+ Response 200 (application/json)
    + Attributes
        + `fiscal_year` (required, number)
        + `toptier_code` (required, string)
        + `agency_budgetary_resources` (required, number, nullable)
        + `prior_year_agency_budgetary_resources` (required, number, nullable)
        + `total_federal_budgetary_resources` (required, number, nullable)
        + `messages` (required, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "fiscal_year": 2020,
                "toptier_code": "020",
                "agency_budgetary_resources": 1829326357849.28,
                "prior_year_agency_budgetary_resources": 1444672550856.93,
                "total_federal_budgetary_resources": 6834681645095.6,
                "messages": []
            }
