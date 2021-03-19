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
        + `parent_agency_toptier_code`: `126` (string, number)
        + `parent_agency_name`: `Department of Defense` (string, number)
