FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient List [/api/v2/award_spending/recipient/{?awarding_agency_id,fiscal_year,limit,page}]

This endpoint lists all all award spending for a given fiscal year and agency id

## GET

This endpoint returns a list of recipients and their amounts.

+ Parameters
    + `awarding_agency_id`: 183 (required, number)
        Internal award id of the recipient you are looking for
    + `fiscal_year`: 2017 (required, number)
        Fiscal Year
    + `limit`: 10 (optional, number)
        The maximum number of results to return in the response.
    + `page`: 1 (optional, number)
        The response page to return (the record offset is (`page` - 1) * `limit`).

+ Response 200 (application/json)
    + Attributes
        + `page_metadata` (PageMetadataObject)
        + `results` (array[RecipientListing], fixed-type)

# Data Structures

## PageMetadataObject (object)
+ `count`: 100 (required, number)
+ `page`: 1 (required, number)
+ `has_next_page`: true (required, boolean)
+ `has_previous_page`: false (required, boolean)
+ `next` (required, string, nullable)
+ `current` (required, string, nullable)
+ `previous` (required, string, nullable)

## RecipientListing (object)
+ `award_category`: `contracts` (required, string)
+ `obligated_amount`: 1000000.01 (required, number)
+ `recipient` (RecipientObject)

        
## RecipientObject (object)
+ `recipient_id`: 346 (required, number)
+ `recipient_name`: `Company Inc.` (required, string)