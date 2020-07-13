FORMAT: 1A
HOST: https://api.usaspending.gov

# Single NAICS And Descendants [/api/v2/references/naics/{code}/]

This endpoint helps traverse a tree stcructure used to power USAspending.gov's NAICS search component on the Advanced Search page.

## GET

This endpoint returns the NAICS specified by "id" and its immediate children.

+ Request A request with a naics id (application/json)
    + Parameters
        + `code`: `11` (optional, number) This will return the requested NAICS and its immediate children.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[NAICSObject], fixed-type)

# Data Structures

## NAICSObject (object)

+ `naics` (required, string)
+ `naics_description` (required, string)
+ `count` (required, number)
+ `children` (optional, array[NAICSObject], fixed-type)