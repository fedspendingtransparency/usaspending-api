FORMAT: 1A
HOST: https://api.usaspending.gov

# State Overview [/api/v2/recipient/state/]

These endpoints are used to power USAspending.gov's state profile pages. This data can be used to visualize the government spending that occurs in a specific state or territory.

## GET

This endpoint returns a list of states and their amounts.

+ Response 200 (application/json)
    + Attributes (array[StateListing], fixed-type)

# Data Structures

## StateListing (object)
+ `name`: `Virginia` (required, string)
+ `code`: `VA` (required, string)
+ `fips`: `51` (required, string)
+ `amount`: 494274.3 (required, number)
+ `type`: `state` (required, enum[string])
    + Members
        + `state`
        + `territory`
        + `district`
