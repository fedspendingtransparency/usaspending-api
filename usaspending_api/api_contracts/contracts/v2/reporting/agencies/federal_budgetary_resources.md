FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies Reporting Federal Budgetary Resources [/api/v2/reporting/agencies/federal_budgetary_resources?{fiscal_year,fiscal_period}]

This endpoint is used to provide information on agencies federal budgetary resources. This data can be used to better understand the ways agencies are alloted monetary resources.

## GET

This endpoint returns federal budgetary resources based on fiscal year and fiscal period.

+ Parameters

    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period` (optional, number)
        The fiscal period.

+ Response 200 (application/json)

    + Attributes (object)
        + `total_budgetary_resources` (required, number)
        + `results` (required, array[FederalBudgetaryResources], fixed-type)
    + Body

            {
                "total_budgetary_resources": 98475827879878972384837,
                "results": [
                    {
                        "name": "Department of Health and Human Services",
                        "abbreviation": "DHHS",
                        "code": "020",
                        "federal_budgetary_resources": 8361447130497.72
                    },
                    {
                        "name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "code": "021",
                        "federal_budgetary_resources": 8361447130497.72
                    }
                ]
            }

# Data Structures

## FederalBudgetaryResources (object)
+ `federal_budgetary_resources` (required, number)
+ `name` (required, string)
+ `abbreviation` (required, string)
+ `code` (required, string)
