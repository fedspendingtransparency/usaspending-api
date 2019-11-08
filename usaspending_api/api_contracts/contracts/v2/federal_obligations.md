FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/federal_obligations/{?fiscal_year,funding_agency_id,limit,page}]

This endpoint is used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

## GET

This endpoint returns the amount that the specific agency has obligated to various federal accounts in a given fiscal year.

+ Parameters
    + `fiscal_year`: 2017 (required, number)
        The fiscal year that you are querying data for.
    + `funding_agency_id`: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.
    + `limit`: 10 (optional, number)
        The maximum number of results to return in the response.
    + `page`: 1 (optional, number)
        The response page to return (the record offset is (`page` - 1) * `limit`).
        
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[FederalAccount], fixed-type)
        + `page_metadata` (required, PageMetadataObject)

# Data Structures

## PageMetadataObject (object)
+ `count` (required, number)
+ `page` (required, number)
+ `has_next_page` (required, boolean)
+ `has_previous_page` (required, boolean)
+ `next` (required, nullable , string)
+ `current` (required, string)
+ `previous` (required, nullable, string)

## FederalAccount (object)
+ `account_title` (required, string)
+ `account_number` (required, string)
+ `id` (required, string)
    The USAspending.gov unique identifier for the federal account. You will need to use this ID when making API requests for details about specific federal accounts.
+ `obligated_amount` (required, string)