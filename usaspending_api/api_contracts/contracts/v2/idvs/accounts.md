FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Federal Accounts

This endpoint powers USAspending.gov's IDV Summary page Federal Accounts visualization section.

## List Federal Accounts [/api/v2/idvs/accounts/]

This endpoint returns a list of federal accounts under a given IDV.

### List Federal Accounts [POST]
+ Request (application/json)
    + Attributes (object)
        + `award_id`:`CONT_IDV_GS30FHA006_4732` (required, string)
            IDV to return accounts for
        + `page`: 1 (optional, number)
            Page number to return
        + `limit`: 10 (optional, number)
            Maximum number to return
        + `order`: `desc` (optional, string)
            Direction of sort
        + `sort`:`total_transaction_obligated_amount` (optional, string)
            The field to sort on
    + Body


            {
                "limit": 10,
                "sort": "total_transaction_obligated_amount",
                "order": "desc",
                "award_id": "CONT_IDV_TMHQ10C0040_2044",
                "page": 1
            }

+ Response 200 (application/json)
    + Attributes (object)
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
+ `funding_agency_abbreviation` (required, string)
+ `funding_agency_name` (required, string)
+ `funding_agency_id` (required, number)
+ `funding_toptier_agency_id` (required, number, nullable)
+ `funding_agency_slug` (required, string, nullable)
