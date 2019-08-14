FORMAT: 1A
HOST: https://api.usaspending.gov

# Glossary [/api/v2/references/glossary/{?limit,page}]

These endpoints are used to power USAspending.gov's glossary components.

## GET

This endpoint returns a list of glossary data.

+ Parameters
    + `page`: `2` (optional, integer)
        Page number to return
    + `limit`: `1` (optional, integer)
        Maximum number to return
        
+ Response 200 (application/json)
    + Attributes (GlossaryListingObject)

# Data Structures

## GlossaryListingObject (object)
+ `page_metadata` (required, PageMetadata)
+ `results` (required, array[GlossaryListing], fixed-type)

## PageMetadata (object)
+ `page`: 2 (required, number)
+ `count`: 129 (required, number)
+ `next`: 3 (required, number, nullable)
+ `previous`: 1 (required, number, nullable)
+ `hasNext`: true (required, boolean)
+ `hasPrevious`: false (required, boolean)

## GlossaryListing (object)
+ `term`: `Acquisition of Assets` (required, string)
+ `slug`: `acquisition-of-assets` (required, string)
+ `data_act_term`: `Acquisition of Assets` (required, string)
+ `plain`: `This major object class includes lorem ipsum` (required, string)
+ `official`: `This major object class covers object classes 31.0 through 33.0 lorem ipsum` (required, string)
+ `resources`: `Learn More: Circular No. A-11` (required, string, nullable)
