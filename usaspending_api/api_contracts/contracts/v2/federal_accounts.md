FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Accounts Landing Page [/api/v2/federal_accounts/]

This endpoint supports the federal account landing page, which provides a list of all federal accounts for which individual federal accounts pages are available on USAspending.gov.

## POST

This endpoint returns a list of federal accounts, their number, name, managing agency, and budgetary resources.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (optional, AdvancedFilterObject)
            The filter takes a fiscal year, but if one is not provided, it defaults to the last certified fiscal year.
        + `sort` (optional, SortObject)
        + `limit` (optional, number)
            The number of results to include per page.
            + Default: 50
        + `page` (optional, number)
            The page of results to return based on the limit.
            + Default: 1
        + `keyword` (optional, string)
            The keyword that you want to search on. Can be used to search by name, number, managing agency, and budgetary resources.
    + Body

            {
                "filters": {
                    "agency_identifier": "339",
                    "fy": "2018"
                },
                "sort": {
                    "direction": "asc",
                    "field": "account_name"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `previous` (optional, number, nullable)
        + `count` (required, number)
        + `limit` (required, number)
        + `hasNext` (boolean, required)
        + `page` (number, required)
        + `hasPrevious` (boolean, required)
        + `next` (number, required, nullable)
        + `fy` (string, required)
        + `results` (array[FederalAccountListing], fixed-type)

# Data Structures

## FederalAccountListing (object)
+ `account_name` (required, string, nullable)
    Name of the federal account. `null` when the name is not provided.
+ `account_number` (required, string, nullable)
    The number for the federal account. `null` when no account number is provided.
+ `account_id` (required, number)
    A unique identifier for the federal account
+ `managing_agency_acronym` (required, string)
+ `agency_identifier` (required, string)
+ `budgetary_resources` (required, number, nullable)
+ `managing_agency` (required, string)

## AdvancedFilterObject (object)
+ `fy` (optional, string)
    Providing `fy` does not change the rows that are returned, instead, it limits the `budgetary_resources` value to the fiscal year indicated.  Federal
    accounts with no submissions for that fiscal year will return null.
    + Default: `previous fiscal year`
+ `agency_identifier` (optional, string)

## SortObject (object)
+ `direction` (optional, enum[string])
    The direction results are sorted by. `asc` for ascending, `desc` for descending.
    + Default: `desc`
    + Members
        + `asc`
        + `desc`
+ `field` (optional, string)
    The field that you want to sort on.
    + Default: `budgetary_resources`
        + Members
            + `budgetary_resources`
            + `account_name`
            + `account_number`
            + `managing_agency`
