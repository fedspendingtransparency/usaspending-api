FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Identifier [/api/v2/autocomplete/accounts/aid/]

This endpoint powers the USAspending.gov Agency Identifier (AID) autocomplete in the Advanced Search -> Program Source -> Treasury and Federal Account filters.

## POST

List of potential Agency Identifiers

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, ComponentFilters)
        + `limit` (optional, number)
            Maximum number of results to return.
            + Default: 10

    + Body

            {
                "filters": {
                    "aid": "02"
                },
                "limit": 3
            }

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[AIDMatch], fixed-type)

    + Body

            {
                "results": [
                    {
                        "aid": "020",
                        "agency_name": "Department of the Treasury",
                        "agency_abbreviation": "TREAS"
                    },
                    {
                        "aid": "021",
                        "agency_name": "Department of the Army",
                        "agency_abbreviation": null
                    },
                    {
                        "aid": "023",
                        "agency_name": "U.S. Tax Court",
                        "agency_abbreviation": "USTAXCOURT"
                    }
                ]
            }

# Data Structures

# AIDMatch (object)
+ `aid` (required, string)
+ `agency_name` (required, string, nullable)
+ `agency_abbreviation` (required, string, nullable)

## ComponentFilters (object)

Each component listed here may be omitted, null, or a string value.  If omitted, no filtering will be performed on that component.  If null, the filter will include account numbers missing that component.  If a string, the filter will perform an exact match on account numbers where that component matches the string provided except in the case of aid where a partial match will be performed on the aid component starting with the provided value.

+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier (3 characters). TAS only.
+ `aid` (optional, string, nullable)
    Agency Identifier (3 characters).
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability (4 characters). TAS only.
+ `epoa` (optional, string, nullable)
    Ending Period of Availability (4 characters). TAS only.
+ `a` (optional, string, nullable)
    Availability Type Code (1 character). Will either be 'X' or null. TAS only.
+ `main` (optional, string, nullable)
    Main Account Code (4 characters).
+ `sub` (optional, string, nullable)
    Sub Account Code (3 characters). TAS only.
