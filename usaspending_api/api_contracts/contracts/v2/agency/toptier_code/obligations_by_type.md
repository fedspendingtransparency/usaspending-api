FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/agency/{toptier_code}/obligations_by_type/{?fiscal_year}]

This endpoint is used to power USAspending.gov's agency profile pages.

## GET

This endpoint returns a breakdown of obligations by award type (contracts, IDVs, grants, loans, direct payments, other) within a fiscal year.

+ Parameters
    + `fiscal_year`: 2017 (required, number)
        The fiscal year that you are querying data for.
        
+ Response 200 (application/json)
    + Attributes (object)
        + `award_obligations` (required, number)
        + `results` (required, array[Obligation], fixed-type)

    + Body

            {
                "award_obligations": 39999999.96,
                "results": [
                    {
                        "code": "B",
                        "obligated_amount": 9999999.99
                    },
                    {
                        "code": "07",
                        "obligated_amount": 9999999.99
                    }
                        {
                        "code": "IDV_B_A",
                        "obligated_amount": 9999999.99
                    },
                    {
                        "code": "05",
                        "obligated_amount": 9999999.99
                    }
                ]
            }

# Data Structures

## Obligation (object)
+ `code` (required, enum[string])
    + Members
        + `02`
        + `03`
        + `04`
        + `05`
        + `06`
        + `07`
        + `08`
        + `09`
        + `10`
        + `11`
        + `A`
        + `B`
        + `C`
        + `D`
        + `IDV_A`
        + `IDV_B`
        + `IDV_B_A`
        + `IDV_B_B`
        + `IDV_B_C`
        + `IDV_C`
        + `IDV_D`
        + `IDV_E`

+ `obligated_amount` (required, number)