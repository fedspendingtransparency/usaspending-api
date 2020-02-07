FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Identifier [/api/v2/autocomplete/accounts/aid/]

This endpoint powers the USAspending.gov Agency Identifier (AID) autocomplete in the Advanced Search -> Program Source -> Treasury and Federal Account filters.

## POST

List of potential Agency Identifiers 

+ Request (application/json)

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

# Data Structure


# AIDMatch (object)
+ `aid` (required, string)
+ `agency_name` (required, string, nullable)
+ `agency_abbreviation` (required, string, nullable)

## ComponentFilters (object)

Each component listed here may be omitted, null, or a string value.  If omitted, no filtering will be performed on that component.  If null, the filter will include account numbers missing that component.  If a string, the filter will perform an exact match on account numbers where that component matches the string provided except in the case of aid where a partial match will be performed on the aid component starting with the provided value.

+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier (020). TAS only.
+ `aid` (optional, string, nullable)
    Agency Identifier (021).
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability (023). TAS only.
+ `epoa` (optional, string, nullable)
    Ending Period of Availability (Main). TAS only.
+ `a` (optional, string, nullable)
    Availability Type Code ('a'). Will either be 'X' or null. TAS only.
+ `main` (optional, string, nullable)
    Main Account Code (Main).
+ `sub` (optional, string, nullable)
    Sub Account Code (Sub). TAS only.
