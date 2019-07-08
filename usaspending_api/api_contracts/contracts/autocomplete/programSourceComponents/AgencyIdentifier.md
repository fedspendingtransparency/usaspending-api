FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Identifier (AID)

This endpoint powers USAspending.gov's Treasury Account and Federal Account Agency Identifier (AID) component filter in the Advanced Search Program Source (TAS) filter.

## Agency Identifier [/api/v2/autocomplete/accounts/aid/]

Returns lists of possible AIDs matching the search string and narrowed down by the given component filters.

## POST
+ Request
    + Attributes (object)
        + filters (required, ComponentFilters)

+ Response 200 (application/json)
    + Attributes (object)
        + results (array[AgencyResult], fixed-type)

# Data Structures

## ComponentFilters (object)
+ aid: `12` (required, string)
    The Agency Identifier search string (max 3 characters).
+ ata: `123` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa: `2017` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ epoa: `2019` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ a: `X` (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ main: `6789` (optional, string, nullable)
    Main Account Code (4 characters). TAS & Federal Account.
+ sub: `098` (optional, string, nullable)
    Sub-Account Code (3 characters). TAS only.

## AgencyResult
+ `aid`: `456` (required, string)
+ `agency_name`: `Department of Sandwiches` (required, string, nullable)
+ `agency_abbreviation`: `DOS` (required, string, nullable)
