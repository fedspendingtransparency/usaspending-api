FORMAT: 1A
HOST: https://api.usaspending.gov

# Funding Accounts

These endpoints are used to power USAspending.gov's IDV Summary Funding Accounts component.

## List Federal Accounts [/api/v2/awards/idvs/accounts/]

This endpoint returns a list of federal accounts under a given IDV.


### List Federal Accounts [POST]
+ Request (application/json)
    + Attributes
        + `award_id`:`CONT_AW_4732_-NONE-_GS30FHA006_-NONE-` (required, string) 
            IDV to return accounts for
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


# Data Structures

## PageMetadata (object)
+ page: 1 (required, number)
+ next: 2 (required, number, nullable)
+ count: 4 (required, number)
+ previous: 1 (required, number, nullable)
+ hasNext: true (required, boolean)
+ hasPrevious: false (required, boolean)

## AccountListing (object)
+ total_transaction_obligated_amount: 1234.56 (required, number)
+ federal_account: `075-1301` (required, string)
+ account_title: `Bureau of Consumer Financial Protection Fund` (required, string)
+ funding_agency_abbreviation: `NIH` (required, string)
+ funding_agency_name: `National Institutes of Health` (required, string)
+ funding_agency_id: 123 (required, number)
