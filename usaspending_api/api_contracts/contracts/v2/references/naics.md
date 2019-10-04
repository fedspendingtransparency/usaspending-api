FORMAT: 1A
HOST: https://api.usaspending.gov

# NAICS

These endpoints are used to power USAspending.gov's NAICS search component on the advanced search page.


## All Tier 1 NAICS [GET /api/v2/references/naics/]

This endpoint returns a list of Tier 1 NAICS codes, their descriptions, and a count of their Tier 3 grandchildren
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[NAICSObject], fixed-type)

## Single NAICS [GET /api/v2/references/naics/{id}/]

This endpoint returns the NAICS specified by "id" and its immediate children.
+ Request A request with a naics id (application/json)
    + Parameters
        + `id`: `11` (optional, number) This will return the required NAICS and its immediate children. 

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[NAICSObject], fixed-type)

## Filtered NAICS [GET /api/v2/references/naics/{?filter}]

This endpoint returns a list of NAICS and their parents/grandparents that match the provided filter.
+ Request A request with a contract id (application/json)
    + Parameters
        + `filter`: `filter=forest` (optional, string) This will filter the NAICS by their descriptions to those matching the text.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[NAICSObject], fixed-type)

## Data Structures

### NAICSObject (object)

+ `naics` (required, string)
+ `naics_description` (required, string)
+ `count` (required, number)
+ `children` (optional, array[NAICSObject], fixed-type)