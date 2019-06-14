FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Source Components

This endpoint powers USAspending.gov's Treasury Account Component and Federal Account Component autocomplete elements in the Advanced Search Program Source (TAS) filter.

## Program Source Components [/api/v2/references/program_source_components/]

Returns lists of available values for some of the components that make up Treasury Account Numbers and Federal Account Numbers.

### Program Source Components [GET]

+ Response 200 (application/json)
    + Attributes (object)
        + agency_ids (required, array[AgencyResult], fixed-type)
        + allocation_transfer_agency_ids (required, array[AgencyResult], fixed-type)

# Data Structures

## AgencyResult (object)
+ cgac: `020` (required, string)
+ agency_name: `Department of the Treasury` (required, string)
+ agency_abbreviation: `TREAS` (required, string, nullable)
