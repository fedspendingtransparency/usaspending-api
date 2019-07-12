FORMAT: 1A
HOST: https://api.usaspending.gov


# Federal Account Profile

# Group Profile Page

These endpoints support individual federal account profile pages.

## Account Overview [/api/v2/federal_accounts/{accountNumber}/]

This endpoint returns the agency identifier, account code, title, and database id for the given federal account.

+ Parameters
    + accountNumber: `011-1022` (required, string)
        The Federal Account symbol comprised of Agency Code and Main Account Code. A unique identifier for federal accounts. 

### Get Account Overview [GET]

+ Response 200 (application/json)
    + Attributes
        + `agency_identifier`: `011` (required, string)
        + `main_account_code`: `1022` (required, string)
        + `federal_account_code`: `011-1022` (required, string)
        + `account_title`: `International Security Assistance` (required, string)
        + id: 1234 (required, number)

# Data Structures
