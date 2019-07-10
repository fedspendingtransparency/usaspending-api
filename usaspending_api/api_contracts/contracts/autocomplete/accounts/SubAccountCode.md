FORMAT: 1A
HOST: https://api.usaspending.gov

# Sub Account Code (SUB)

This endpoint powers USAspending.gov's Treasury Account Sub Account Code (SUB) component filter in the Advanced Search Program Source (TAS) filter.

## Sub Account Code [/api/v2/autocomplete/accounts/sub/]

Returns lists of possible SUBs narrowed down by the given component filters. Performs a partial search on `sub` and an exact search on the rest of the filters.

## POST
+ Request (application/json)
    + Attributes (object)
        + filters (required, ComponentFilters)
        + limit (required, number)
            Maximum number of results to return.
            + Default: 10

+ Response 200 (application/json)
    + Attributes (object)
        + results: (array[string])

    + Body

            {
                "results": [
                    "098",
                    "097",
                    "609"
                ]
            }

# Data Structures

## ComponentFilters (object)
+ sub (optional, string, nullable)
    Sub-Account Code search string (max 3 characters). Excluding this field returns all SUBs.
+ aid (optional, string, nullable)
    The Agency Identifier (3 characters).
+ ata (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ epoa (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ a (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ main (optional, string, nullable)
    Main Account Code (4 characters).
