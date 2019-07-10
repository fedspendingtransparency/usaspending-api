FORMAT: 1A
HOST: https://api.usaspending.gov

# Allocation Transfer Agency Id (ATA)

This endpoint powers USAspending.gov's Treasury Account Allocation Transfer Agency Id (ATA) component filter in the Advanced Search Program Source (TAS) filter.

## Allocation Transfer Agency Id [/api/v2/autocomplete/accounts/ata/]

Returns lists of possible ATAs narrowed down by the given component filters. Performs a partial search on `ata` and an exact search on the rest of the filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)

+ Response 200 (application/json)
    + Attributes (object)
        + results (array[AgencyResult], fixed-type)

# Data Structures

## ComponentFilters (object)
+ ata: `12` (required, string)
    The Allocation Transfer Agency Identifier search string (max 3 characters).
+ aid: `123` (optional, string, nullable)
    Agency Identifier (3 characters).
+ bpoa: `2017` (optional, string, nullable)
    Beginning Period of Availability (4 characters).
+ epoa: `2019` (optional, string, nullable)
    Ending Period of Availability (4 characters).
+ a: `X` (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null.
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters).
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters).

## AgencyResult
+ `ata`: `456` (required, string)
+ `agency_name`: `Department of Sandwiches` (required, string, nullable)
+ `agency_abbreviation`: `DOS` (required, string, nullable)
