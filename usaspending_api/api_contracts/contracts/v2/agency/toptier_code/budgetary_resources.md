FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/agency/{toptier_code}/budgetary_resources/{?fiscal_year}]

Returns budgetary resources and obligations for the agency and fiscal year requested.

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
        + `agency_total_obligated` (required, number, nullable)
        + `agency_obligation_by_period` (required, array, fixed-type)
            + (object)
                + `period` (required, number)
                + `obligated` (required, number)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "fiscal_year": 2019,
                "toptier_code": "012",
                "agency_budgetary_resources": 242452364827.19,
                "prior_year_agency_budgetary_resources": 224393842858.3,
                "total_federal_budgetary_resources": 7885819024455.14,
                "agency_total_obligated": 170897830109.04,
                "agency_obligation_by_period": [
                    {
                        "period": 3,
                        "obligated": 46698411999.28
                    },
                    {
                        "period": 6,
                        "obligated": 85901744451.98
                    },
                    {
                        "period": 9,
                        "obligated": 120689245470.66
                    },
                    {
                        "period": 12,
                        "obligated": 170898908395.86
                    }
                ],
                "messages": []
            }
