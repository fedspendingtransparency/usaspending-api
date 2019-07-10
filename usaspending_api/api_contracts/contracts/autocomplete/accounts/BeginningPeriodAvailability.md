FORMAT: 1A
HOST: https://api.usaspending.gov

# Beginning Period of Availability (BPOA)

This endpoint powers USAspending.gov's Treasury Account Beginning Period of Availability (BPOA) component filter in the Advanced Search Program Source (TAS) filter.

## Beginning Period of Availability [/api/v2/autocomplete/accounts/bpoa/]

Returns lists of possible BPOAs narrowed down by the given component filters. Performs a partial search on `bpoa` and an exact search on the rest of the filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)
        + limit (required, number)
            Maximum number of results to return.
            + Default: 10

+ Response 200 (application/json)
    + Attributes (object)
        + results: `2015`, `2016`, `2017` (array[string])

# Data Structures

## ComponentFilters (object)
+ bpoa: `20` (optional, string, nullable)
    Beginning Period of Availability search string (max 4 characters). Excluding this field returns all BPOAs.
+ aid: `12` (optional, string, nullable)
    The Agency Identifier (3 characters).
+ ata: `123` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ epoa: `2019` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ a: `X` (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters). TAS & Federal Account.
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters). TAS only.
