FORMAT: 1A
HOST: https://api.usaspending.gov

# Subawards [/api/v2/subawards/]

This endpoint returns a list of data that is associated with the award profile page.

## List filtered subawards [POST /api/v2/subawards/]

This endpoint returns a filtered set of subawards.

+ Request (application/json)
    + Attributes (object)
        + `page` (required, number)
            + Default: 1
        + `limit` (optional, number)
            + Default: 10
        + `sort` (required, enum[string], fixed-type)
            + Members
                + subaward_number
                + id
                + description
                + action_date
                + amount
                + recipient_name
                + award_id
        + `order` (required, enum[string], fixed-type)
            + Members
                + asc
                + desc 
            + Default: desc
        + `award_id` (optional, number)
            Award ID of parent Award
            
+ Response 200 (application/json)
    + Attributes
        + `results` (required, array[SubawardResponse], fixed-type)
        + `page_metadata` (required, PageMetadataObject)

# Data Structures

## SubawardResponse (object)
+ `subaward_number` (required, string)
+ `amount` (required, number)
+ `id` (required, number)
+ `action_date` (required, string)
+ `recipient_name` (required, string)
+ `description` (required, string)

## PageMetadataObject (object)
+ page (required, number)
+ next (required, number, nullable)
+ previous (required, number, nullable)
+ hasNext (required, boolean)
+ hasPrevious (required, boolean)

