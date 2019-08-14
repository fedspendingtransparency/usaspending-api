FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Profile [/api/v2/financial_balances/agencies/{?fiscal_year,funding_agency_id}]

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

## GET

This endpoint returns aggregated balances for a specific government agency in a given fiscal year.

+ Parameters
    + `fiscal_year`: 2017 (required, number)
        The fiscal year that you are querying data for.
    + `funding_agency_id`: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[FinancialBalance], fixed-type)

# Data Structures

## FinancialBalance (object)
+ `budget_authority_amount`: `1899160740172.16` (required, string)
+ `obligated_amount`: `524341511584.82` (required, string)
+ `outlay_amount`: `523146830716.62` (required, string)

