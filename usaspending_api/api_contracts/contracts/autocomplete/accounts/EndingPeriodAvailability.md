FORMAT: 1A
HOST: https://api.usaspending.gov

# Ending Period of Availability (EPOA)

This endpoint powers USAspending.gov's Treasury Account Ending Period of Availability (EPOA) component filter in the Advanced Search Program Source (TAS) filter.

## Ending Period of Availability [/api/v2/autocomplete/accounts/epoa/]

Returns lists of possible EPOAs narrowed down by the given component filters. Performs a partial search on `epoa` and an exact search on the rest of the filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)

+ Response 200 (application/json)
    + Attributes (object)
        + results: `2015`, `2016`, `2017` (array[string])

+ Body

# Data Structures

## ComponentFilters (object)
+ epoa: `20` (required, string)
    Ending Period of Availability search string (max 4 characters).
+ aid: `12` (optional, string, nullable)
    The Agency Identifier (3 characters).
+ ata: `123` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa: `2019` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ a: `X` (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters). TAS & Federal Account.
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters). TAS only.
