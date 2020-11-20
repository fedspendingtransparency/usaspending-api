FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Federal Budgetary Resources [/api/v2/reporting/agencies/{agency_code}/federal_budgetary_resources?{fiscal_year,fiscal_period}]

This endpoint is used to provide information on an agencies federal budgetary resources. This data can be used to better understand the way an agency is alloted monetary resources.

## GET

This endpoint returns federal budgetary resources based on fiscal year and fiscal period.

+ Parameters

    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period` (optional, number)
        The fiscal period.
    + `page` (optional, number)
        The page of results to return based on the limit.
        + Default: 1
    + `limit` (optional, number)
        The number of results to include per page.
        + Default: 10
    + `order` (optional, enum[string])
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `asc`
        + Members
            + `asc`
            + `desc`

+ Response 200 (application/json)

    + Attributes (object)
        + `total_budgetary_resources` (required, number)
        + `results` (required, array[FederalBudgetaryResources], fixed-type)
    + Body

            {
                "page_metadata": {
                      "page": 1,
                      "next": 2,
                      "previous": 0,
                      "hasNext": false,
                      "hasPrevious": false,
                      "total": 2,
                      "limit": 10
                },
                "total_budgetary_resources": 98475827879878972384837,
                "results": [
                    {
                        "name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "code": "020",
                        "federal_budgetary_resources": 8361447130497.72,
                        "fiscal_year": 2020,
                        "fiscal_period": 6
                    },
                    {
                        "name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "code": "021",
                        "federal_budgetary_resources": 234525.72,
                        "fiscal_year": 2020,
                        "fiscal_period": 5
                    }
                ]
            }

# Data Structures

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## FederalBudgetaryResources (object)
+ `federal_budgetary_resources` (required, number)
+ `name` (required, string)
+ `abbreviation` (required, string)
+ `code` (required, string)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
