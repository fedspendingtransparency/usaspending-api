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

    + Body

            {
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                },
                "results": [
                    {
                        "id": 119270129,
                        "subaward_number": "Z981002",
                        "description": "DEVELOPMENT OF A SELF-SUSTAINED WIRELESS INTEGRATED STRUCTURAL HEALTH MONITORING SYSTEM FOR HIGHWAY BRIDGES",
                        "action_date": "2011-10-27",
                        "amount": 110000.0,
                        "recipient_name": "URS GROUP, INC."
                    }
                ]
            }

# Data Structures

## SubawardResponse (object)
+ `id` (required, number)
+ `subaward_number` (required, string)
+ `description` (required, string)
+ `action_date` (required, string)
+ `amount` (required, number)
+ `recipient_name` (required, string)

## PageMetadataObject (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

