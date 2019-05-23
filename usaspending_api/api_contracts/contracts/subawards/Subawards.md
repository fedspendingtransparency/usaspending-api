FORMAT: 1A
HOST: https://api.usaspending.gov

# Subawards

These endpoints are used to power USAspending.gov's subaward listings.

# Subaward List

This endpoint returns a filtered set of subawards.

## Subawards [/api/v2/subawards/]

This endpoint returns a list of data that is associated with the award profile page.

### Subawards [POST]

+ Request (application/json)
    + Attributes (object)
        + page: 1 (required, number)
        + limit: 10 (optional, number)
        + filters (required, FilterObject)
        + order: (required, enum)
            + asc
            + desc 
            
+ Response 200 (application/json)
    + Attributes
        + `limit` (optional, number)
        + `results`(array[SubawardResponse])
        + `page_metadata`(PageMetadataObject)

# Data Structures

## SubawardResponse (object)
+ subaward_number (required, string)
+ amount (required, number)
+ id (required, number)
+ action_date (required, string)
+ recipient_name (required, string)
+ description (required, string)

## PageMetadataObject (object)
+ page: 1 (required, number)
+ hasNext (required, boolean)
+ hasPrevious (required, boolean)

## FilterObject (object)
+ field (required, string)
+ operation (required, string)
+ value (required, number)
