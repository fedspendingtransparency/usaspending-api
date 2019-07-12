FORMAT: 1A
HOST: https://api.usaspending.gov

# Glossary

These endpoints are used to power USAspending.gov's glossary components. 



## List Glossary [/api/v2/references/glossary/{?limit,page}]

This endpoint returns a list of glossary data.

+ Parameters
    + `page`: `1` (optional, integer)
        Page number to return
    + `limit`: `10` (optional, integer)
        Maximum number to return

### List Glossary [GET]

+ Response 200 (application/json)
    + Attributes (GlossaryListingObject)


# Data Structures

## GlossaryListingObject (object)
+ page_metadata:(required, PageMetadata)
+ results:( required, array[GlossaryListing])

## PageMetadata (object)
+ page: 1 (required, number)
+ count: 10 (required, number)
+ next: 2 (required, number, nullable)
+ previous: 1 (required, number, nullable)
+ hasNext: true (required, boolean)
+ hasPrevious: false (required, boolean)

## GlossaryListing (object)
+ term: Acquisition of Assets (required, string)
+ slug: acquisition-of-assets (required, string)
+ data_act_term: Acquisition of Assets (required, string)
+ plain: This major object class includes lorem ipsum (required, string)
+ official : This major object class covers object classes 31.0 through 33.0 lorem ipsum (required, string)
+ resources: Learn More: Circular No. A-11 (required, string, nullable)

## GlossaryAutocompleteObject (object)
+ results: (required, array[string])
+ count: 4 (required, number)
+ search_text: ab (required, string)
+ matched_terms:(required, array[GlossaryListing])
