FORMAT: 1A
HOST: https://api.usaspending.gov

# NAICS List [/api/v2/references/naics/{?filter}]

Used to power USAspending.gov's NAICS search component on the Advanced Search page.


## GET

This endpoint returns a list of Tier 1 NAICS codes, their descriptions, and a count of their Tier 3 grandchildren. If filter is provided then return parents/grandparents that match the search text.
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