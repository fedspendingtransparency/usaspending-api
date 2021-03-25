FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/agency/{toptier_code}/budgetary_resources/]

Returns budgetary resources and obligations for the agency and fiscal year requested.

## GET

+ Parameters
    + `toptier_code`: 075 (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `agency_data_by_year` (required, AgencyData, fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "075",
                "agency_by_year": [
                    {
                        "fiscal_year": 2021,
                        "agency_budgetary_resources": 2312064788963.38,
                        "agency_total_obligated": 1330370762556.73,
                        "total_budgetary_resources": 2726162666269.23
                    },
                    {
                        "fiscal_year": 2020,
                        "agency_budgetary_resources": 14011153816723.11,
                        "agency_total_obligated": 8517467330750.3,
                        "total_budgetary_resources": 68664861885470.66
                    },
                    {
                        "fiscal_year": 2019,
                        "agency_budgetary_resources": 7639156008853.84,
                        "agency_total_obligated": 4458093517698.44,
                        "total_budgetary_resources": 34224736936338.08
                    },
                    {
                        "fiscal_year": 2018,
                        "agency_budgetary_resources": 6503160322408.84,
                        "agency_total_obligated": 4137177463626.79,
                        "total_budgetary_resources": 28449025364570.94
                    },
                    {
                        "fiscal_year": 2017,
                        "agency_budgetary_resources": 4994322260247.61,
                        "agency_total_obligated": 3668328859224.09,
                        "total_budgetary_resources": 18710078230235.09
                    }
                ],
                "messages": []
            }

# Data Structures
## AgencyData (object)
+ `fiscal_year` (required, number)
+ `agency_budgetary_resources` (required, number, nullable)
+ `total_budgetary_resources` (required, number, nullable)
+ `agency_total_obligated` (required, number, nullable)