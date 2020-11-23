FORMAT: 1A
HOST: https://api.usaspending.gov

# Total Government Budgetary Resources [/api/v2/references/total_budgetary_resources?{fiscal_year,fiscal_period}]

This endpoint is used to provide information on the federal budgetary resources of the government.

## GET

This endpoint returns federal budgetary resources based on fiscal year and fiscal period.

+ Parameters

    + `fiscal_year`(optional, number)
        The fiscal year.
    + `fiscal_period` (optional, number)
        The fiscal period.

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[FederalBudgetaryResources], fixed-type)
    + Body

            {
                "results": [
                    {
                        "total_budgetary_resources": 8361447130497.72,
                        "fiscal_year": 2020,
                        "fiscal_period": 6
                    },
                    {
                        "total_budgetary_resources": 234525.72,
                        "fiscal_year": 2020,
                        "fiscal_period": 5
                    }
                ]
            }

# Data Structures

## FederalBudgetaryResources (object)
+ `total_budgetary_resources` (required, number)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
