FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Accounts Landing Page [/api/v2/federal_accounts/{account_number}/]

This endpoint supports the federal account landing page, which provides a list of all federal accounts for which individual federal accounts pages are available on USAspending.gov.

## GET

This endpoint returns the agency identifier, account code, title, and database id for the given federal account.

+ Parameters
    + `account_number`: `011-1022` (required, string)
        The Federal Account symbol comprised of Agency Code and Main Account Code. A unique identifier for federal accounts.

+ Response 200 (application/json)
    + Attributes
        + `agency_identifier`: `011` (required, string)
        + `main_account_code`: `1022` (required, string)
        + `federal_account_code`: `011-1022` (required, string)
        + `account_title`: `International Security Assistance` (required, string)
        + `id`: 1234 (required, number)

# Data Structures

## FederalAccountListing (object)
+ `account_name`: `Public Debt Principal (Indefinite), Treasury` (required, string, nullable)
    Name of the federal account. `null` when the name is not provided.
+ `account_number`: `023-2342` (required, string, nullable)
    The number for the federal account. `null` when no DUNS is provided.
+ `account_id`: 5354 (required, number)
    A unique identifier for the federal account
+ `managing_agency_acronym`: `BEM` (required, string)
+ `agency_identifier`: `032` (required, string)
+ `budgetary_resources`: 3423.23 (required, number)
+ `managing_agency`: `Central Intelligence Agency` (required, string)

## TimeFilterObject (object)
+ `fy`: `2018` (required, string)

## SortObject (object)
+ `field`: `budgetary_resources` (optional, string)
    The field that you want to sort on.
    + Default: `budgetary_resources`
        + Members
            + `budgetary_resources`
            + `account_name`
            + `account_number`
            + `managing_agency`
+ `direction`: `desc` (optional, string)
    The direction results are sorted by. `asc` for ascending, `desc` for descending.
    + Default: `desc`