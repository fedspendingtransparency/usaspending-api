FORMAT: 1A
HOST: https://api.usaspending.gov

# Sub Account Code (SUB)

This endpoint powers USAspending.gov's Treasury Account Sub Account Code (SUB) component filter in the Advanced Search Program Source (TAS) filter.

## Sub Account Code [/api/v2/autocomplete/accounts/sub/]

Returns lists of possible SUBs matching the search string and narrowed down by the given component filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)

+ Response 200 (application/json)
    + Attributes (object)
        + results: `098`, `097`, `609` (array[string])

# Data Structures

## ComponentFilters (object)
+ sub: `09` (required, string)
    Sub-Account Code search string (max 3 characters).
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
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters).
