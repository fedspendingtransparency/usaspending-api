FORMAT: 1A
HOST: https://api.usaspending.gov

# Total Government Budgetary Resources [/api/v2/references/total_budgetary_resources/{?fiscal_year,fiscal_period}]

This endpoint is used to provide information on the federal budgetary resources of the government.

## GET

This endpoint returns federal budgetary resources by fiscal year and fiscal period.

+ Parameters

    + `fiscal_year` (optional, number)
        The fiscal year to retrieve, 2017 or later.
    + `fiscal_period` (optional, number)
        The fiscal period. If this optional parameter is provided then `fiscal_year` is a required parameter. If `fiscal_period` is provided without `fiscal_year`, a 400 error is returned.  Valid values: 2-12 (2 = November ... 12 = September). For retrieving quarterly data, provide the period which equals 'quarter * 3' (e.g. Q2 = P6). If neither paramater is provided, the entire available history will be returned.

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[FederalBudgetaryResources], fixed-type); sorted by fiscal year, fiscal period
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "results": [
                    {
                        "fiscal_year": 2020,
                        "fiscal_period": 6
                        "total_budgetary_resources": 8361447130497.72,
                    },
                    {
                        "fiscal_year": 2020,
                        "fiscal_period": 5
                        "total_budgetary_resources": 234525.72,
                    }
                ],
                "messages": []
            }

# Data Structures

## FederalBudgetaryResources (object)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
+ `total_budgetary_resources` (required, number)
