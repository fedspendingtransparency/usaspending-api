FORMAT: 1A
HOST: https://api.usaspending.gov

# Availability Type Code [/api/v2/autocomplete/accounts/a/]

This endpoint powers the USAspending.gov Availability Type Code (A) autocomplete in the Advanced Search -> Program Source -> Treasury Account filter.

## POST

List of potential Availability Type Codes

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, ComponentFilters)
        + `limit` (optional, number)
            Maximum number of results to return.
            + Default: 10

    + Body

            {
                "filters": {
                    "aid": "000"
                },
                "limit": 3
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array, fixed-type)
            + (string, nullable)

    + Body

            {
                "results": [
                    "X",
                    null
                ]
            }

# Data Structures

## ComponentFilters (object)

Each component listed here may be omitted, null, or a string value.  If omitted, no filtering will be performed on that component.  If null, the filter will include account numbers missing that component.  If a string, the filter will perform an exact match on account numbers where that component matches the string provided.

+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ `aid` (optional, string, nullable)
    Agency Identifier (3 characters).
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ `epoa` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ `a` (optional, string, nullable)
    Availability Type Code (1 character). Will either be 'X' or null. TAS only.
+ `main` (optional, string, nullable)
    Main Account Code (4 characters).
+ `sub` (optional, string, nullable)
    Sub Account Code (3 characters). TAS only.
