FORMAT: 1A
HOST: https://api.usaspending.gov

# Main Account Code (MAIN)

This endpoint powers USAspending.gov's Treasury Account and Federal Account Main Account Code (MAIN) component filter in the Advanced Search Program Source (TAS) filter.

## Main Account Code [/api/v2/autocomplete/accounts/main/]

Returns lists of possible MAINs narrowed down by the given component filters. Performs a partial search on `main` and an exact search on the rest of the filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)
        + limit (required, number)
            Maximum number of results to return.
            + Default: 10

+ Response 200 (application/json)
    + Attributes (object)
        + results: `6789`, `4567`, `5678` (array[string])

# Data Structures

## ComponentFilters (object)
+ main: `67` (optional, string, nullable)
    Main Account Code search string (max 4 characters). Excluding this field returns all MAINs.
+ aid: `12` (optional, string, nullable)
    The Agency Identifier (3 characters).
+ ata: `123` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa: `2019` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ epoa: `2019` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ a: `X` (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters). TAS only.
