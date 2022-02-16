FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Federal Accounts

These endpoints are used to power USAspending.gov's Award Summary page Federal Accounts visualization section.

## List Federal Accounts [/api/v2/awards/accounts/]

This endpoint returns a list of federal accounts under a given award.

### List Federal Accounts [POST]
+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `award_id` (required, string) - Award to return accounts for
        + `page` (optional, number) - Page number to return
        + `limit` (optional, number) - Maximum number to return
        + `order` (optional, enum[string]) - Direction of sort
            + Members
                + `asc`
                + `desc`
            + Default
                + `desc`
        + `sort` (optional, enum[string]) - The field to sort on
            + Members
                + `account_title`
                + `agency`
                + `federal_account`
                + `total_transaction_obligated_amount`
            + Default
                + `federal_account`
    + Body

            {
                "limit": 10,
                "sort": "total_transaction_obligated_amount",
                "order": "desc",
                "award_id": "CONT_AWD_N0001917C0001_9700_-NONE-_-NONE-",
                "page": 1
            }

+ Response 200 (application/json)
    + Attributes
        + `results` (required, array[AccountListing], fixed-type)
        + `page_metadata` (required, PageMetadata, fixed-type)
    + Body

            {
                "results": [
                    {
                        "total_transaction_obligated_amount": 1234.56,
                        "federal_account": "075-1301",
                        "account_title": "Bureau of Consumer Financial Protection Fund",
                        "funding_agency_abbreviation": "NIH",
                        "funding_agency_name": "National Institutes of Health",
                        "funding_agency_id": 123,
                        "funding_toptier_agency_id": 45,
                        "funding_agency_slug": "national-institutes-of-health"
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "count": 4,
                    "previous": 1,
                    "hasNext": true,
                    "hasPrevious": false
                }
            }


# Data Structures

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `count` (required, number)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

## AccountListing (object)
+ `total_transaction_obligated_amount` (required, number)
+ `federal_account` (required, string)
+ `account_title` (required, string)
+ `funding_agency_abbreviation` (required, string, nullable)
+ `funding_agency_name` (required, string, nullable)
+ `funding_agency_id` (required, number, nullable)
+ `funding_toptier_agency_id` (required, number, nullable)
+ `funding_agency_slug` (required, string, nullable)
