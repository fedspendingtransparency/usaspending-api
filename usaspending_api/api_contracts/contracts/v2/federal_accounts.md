FORMAT: 1A
HOST: https://api.usaspending.gov


# Federal Account Landing Page

This endpoint supports the federal account landing page, which provides a list of all federal accounts for which individual federal accounts pages are available on USAspending.gov.

## List Federal Accounts [/api/v2/federal_accounts/]

This endpoint returns a list of federal accounts, their number, name, managing agency, and budgetary resources.

### List Federal Accounts [POST]

+ Request (application/json)
    + Attributes (object)
        + `filters` (optional, TimeFilterObject)
            The filter takes a fiscal year, but if one is not provided, it defaults to the last certified fiscal year.
        + `sort` (optional, SortObject)
        + `limit`: 50 (optional, number)
            The number of results to include per page.
            + Default: 50
        + `page`: 1 (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `keyword`: test (optional, string)
            The keyword that you want to search on. Can be used to search by name, number, managing agency, and budgetary resources.

+ Response 200 (application/json)
    + Attributes
        + `previous` (optional, number, nullable)
        + `count`: 1 (required, number)
        + `limit`: 50 (required, number)
        + `hasNext`: false (boolean, required)
        + `page`: 1 (number, required)
        + `hasPrevious`: false (boolean, required)
        + `next`: 2 (number, required, nullable)
        + `fy`: `2018` (string, required)
        + `results` (array[FederalAccountListing], fixed-type)
 
# Federal Account Profile

## Group Profile Page

This endpoint supports individual federal account profile pages.

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

## FederalAccountListing (object)
+ `account_name`: `Public Debt Principal (Indefinite), Treasury` (required, string, nullable)
    Name of the federal account. `null` when the name is not provided.
+ `account_number`: `023-2342` (required, string, nullable)
    The number for the federal account. `null` when no DUNS is provided.
+ `account_id`: 5354 (required, number)
    A unique identifier for the federal account
+ `managing_agency_acronym`: `BEM` (required, string)
+ `agency_identifier`: `032` (required, string)
+ `budgetary_resources`: 03423.23 (required, number)
+ `managing_agency`: `Central Intelligence Agency` (required, string)

## TimeFilterObject (object)
+ fy: `2018` (required, string)

## SortObject (object)
+ field: `budgetary_resources` (optional, string)
    The field that you want to sort on.
    + Default: `budgetary_resources`
        + Members
            + `budgetary_resources`
            + `account_name`
            + `account_number`
            + `managing_agency`
+ direction: `desc` (optional, string)
    The direction results are sorted by. `asc` for ascending, `desc` for descending.
    + Default: `desc`