FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Federal Accounts

These endpoints are used to power USAspending.gov's Award Summary page Federal Accounts visualization section.

## List Federal Accounts [/api/v2/awards/accounts/]

This endpoint returns a list of federal accounts under a given award.

### List Federal Accounts [POST]
+ Request (application/json)
    + Attributes
        + `award_id`:`CONT_AWD_DEAC5206NA25396_8900_-NONE-_-NONE-` (required, string)
            Award to return accounts for
        + `page`: 1 (optional, number)
            Page number to return
        + `limit`: 10 (optional, number)
            Maximum number to return
        + `order`: `desc` (optional, string)
            Direction of sort
        + `sort`:`total_transaction_obligated_amount` (optional, string)
            The field to sort on

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
                    "funding_agency_id": 123
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
+ page (required, number)
+ next (required, number, nullable)
+ count (required, number)
+ previous (required, number, nullable)
+ hasNext (required, boolean)
+ hasPrevious (required, boolean)

## AccountListing (object)
+ total_transaction_obligated_amount (required, number)
+ federal_account (required, string)
+ account_title (required, string)
+ funding_agency_abbreviation (required, string)
+ funding_agency_name (required, string)
+ funding_agency_id (required, number)
