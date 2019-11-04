FORMAT: 1A
HOST: https://api.usaspending.gov

# Fiscal Year Snapshot [/api/v2/federal_accounts/{federal_account_id}/fiscal_year_snapshot/{fiscal_year}/]

## GET

This route sends a request to the backend to retrieve budget information for a federal account.  If no fiscal year is supplied, the federal account's most recent fiscal year is used.

+ Parameters
    + `federal_account_id`: 6000 (required, number)
    + `fiscal_year`: 2018 (required, number)

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, object)
            + `outlay` (required, number)
            + `budget_authority` (required, number)
            + `unobligated` (required, number)
            + `balance_brought_forward` (required, number)
            + `other_budgetary_resources` (required, number)
            + `appropriations` (required, number)
            + `name` (required, string)
    + Body

            {
                "results": {
                    "other_budgetary_resources": 0.0,
                    "balance_brought_forward": 970000000.0,
                    "outlay": 0.0,
                    "name": "Foreign Currency Fluctuations, Defense",
                    "appropriations": 0.0,
                    "unobligated": 970000000.0,
                    "budget_authority": 970000000.0,
                    "obligated": 0.0
                }
            }
