FORMAT: 1A
HOST: https://api.usaspending.gov

# Fiscal Year Snapshot [/api/v2/federal_accounts/{federal_account_id}/fiscal_year_snapshot/]

## GET

This route sends a request to the backend to retrieve budget information for a federal account.  If no fiscal year is supplied, the federal account's most recent fiscal year is used.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `federal_account_id`:  6000 (required, number)
            Database id for a federal account.

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