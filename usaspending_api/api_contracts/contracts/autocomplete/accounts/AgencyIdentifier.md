FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Identifier (AID)

This endpoint powers USAspending.gov's Treasury Account and Federal Account Agency Identifier (AID) component filter in the Advanced Search Program Source (TAS) filter.

## Agency Identifier [/api/v2/autocomplete/accounts/aid/]

Returns lists of possible AIDs narrowed down by the given component filters. Performs a partial search on `aid` and an exact search on the rest of the filters.

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
+ aid (optional, string, nullable)
    The Agency Identifier search string (max 3 characters). Excluding this field returns all AIDs.
+ ata (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ bpoa (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ epoa (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ a (optional, string, nullable)
    Availability Type Code (1 character) - will either be 'X' or null. TAS only.
+ main (optional, string, nullable)
    Main Account Code (4 characters). TAS & Federal Account.
+ sub (optional, string, nullable)
    Sub-Account Code (3 characters). TAS only.

## AgencyResult (object)
+ `aid` (required, string)
+ `agency_name` (required, string, nullable)
+ `agency_abbreviation` (required, string, nullable)
