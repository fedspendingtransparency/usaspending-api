FORMAT: 1A
HOST: https://api.usaspending.gov

# Subawards [/api/v2/subawards/]

This endpoint returns a list of data that is associated with the award profile page.

## POST

This endpoint returns a filtered set of subawards.

+ Request (application/json)
    + Attributes (object)
        + `page` (required, number)
            + Default: 1
        + `limit` (optional, number)
            + Default: 10
        + `sort` (required, enum[string], fixed-type)
            + Members
                + `subaward_number`
                + `id`
                + `description`
                + `action_date`
                + `amount`
                + `recipient_name`
        + `order` (required, enum[string], fixed-type)
            + Members
                + `asc`
                + `desc`
            + Default: `desc`
        + `award_id` (optional, string)
            Either a "generated" natural award id (string) or a database surrogate award id (number).  Generated award identifiers are preferred as they are effectively permanent.  Surrogate award ids are retained for backward compatibility but are deprecated.
    + Body
            
            
            {
                "page": 1,
                "limit": 15,
                "sort": "subaward_number",
                "order": "desc",
                "award_id": "25882628"
            }
        
            
+ Response 200 (application/json)
    + Attributes (object)
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
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

