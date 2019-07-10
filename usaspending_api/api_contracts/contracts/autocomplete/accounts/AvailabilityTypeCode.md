FORMAT: 1A
HOST: https://api.usaspending.gov

# Availability Type Code (A)

This endpoint powers USAspending.gov's Treasury Account Availability Type Code (A) component filter in the Advanced Search Program Source (TAS) filter.

## Availability Type Code [/api/v2/autocomplete/accounts/a/]

Returns lists of possible As narrowed down by the given component filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)

+ Response 200 (application/json)
    + Attributes (object)
        + results: `X` (array[string])

# Data Structures

## ComponentFilters (object)
+ a: `X` (required, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null.
+ aid: `12` (optional, string, nullable)
    The Agency Identifier (3 characters).
+ ata: `123` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa: `2019` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ epoa: `2019` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters).
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters).
