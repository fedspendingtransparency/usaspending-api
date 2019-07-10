FORMAT: 1A
HOST: https://api.usaspending.gov

# Allocation Transfer Agency Id (ATA)

This endpoint powers USAspending.gov's Treasury Account Allocation Transfer Agency Id (ATA) component filter in the Advanced Search Program Source (TAS) filter.

## Allocation Transfer Agency Id [/api/v2/autocomplete/accounts/ata/]

Returns lists of possible ATAs narrowed down by the given component filters. Performs a partial search on `ata` and an exact search on the rest of the filters.

## POST
+ Request (application/json)
    + Attributes (object)
        + filters (required, ComponentFilters)
        + limit (required, number)
            Maximum number of results to return.
            + Default: 10

+ Response 200 (application/json)
    + Attributes (object)
        + results (array[AgencyResult], fixed-type)

    + Body

            {
                "results": [
                    {
                        "aid": "456",
                        "agency_name": "Mock Agency 1",
                        "agency_abbreviation": "ABC"
                    },
                    {
                        "aid": "789",
                        "agency_name": "Mock Agency 2",
                        "agency_abbreviation": null
                    }
                ]
            }

# Data Structures

## ComponentFilters (object)
+ ata (optional, string, nullable)
    The Allocation Transfer Agency Identifier search string (max 3 characters). Excluding this field returns all ATAs.
+ aid (optional, string, nullable)
    Agency Identifier (3 characters).
+ bpoa (optional, string, nullable)
    Beginning Period of Availability (4 characters).
+ epoa (optional, string, nullable)
    Ending Period of Availability (4 characters).
+ a (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null.
+ main (optional, string, nullable)
    Main Account Code (4 characters).
+ sub (optional, string, nullable)
    Sub-Account Code (3 characters).

## AgencyResult (object)
+ `ata` (required, string)
+ `agency_name` (required, string, nullable)
+ `agency_abbreviation` (required, string, nullable)
