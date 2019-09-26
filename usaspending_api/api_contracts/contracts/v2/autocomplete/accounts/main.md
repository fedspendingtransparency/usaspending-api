FORMAT: 1A
HOST: https://api.usaspending.gov

# Main Account Code [/api/v2/autocomplete/accounts/main/]

This endpoint powers the USAspending.gov Main Account Code (MAIN) autocomplete in the Advanced Search -> Program Source -> Treasury and Federal Account filters.

## POST

List of potential Main Account Codes 

+ Request (application/json)

    + Attributes (object)
        + `filters` (required, ComponentFilters)
        + `limit` (optional, number)
            Maximum number of results to return.
            + Default: 10

    + Body

            {
                "filters": {
                    "main": "30"
                },
                "limit": 3
            }

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array, fixed-type)
            + (string)

    + Body

            {
                "results": [
                    "3010",
                    "3011",
                    "3020"
                ]
            }

# Data Structures

## ComponentFilters (object)

Each component listed here may be omitted, null, or a string value.  If omitted, no filtering will be performed on that component.  If null, the filter will include account numbers missing that component.  If a string, the filter will perform an exact match on account numbers where that component matches the string provided except in the case of main where a partial match will be performed on the main component starting with the provided value.

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
