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
        + `total_obligations` (required, number)
        + `results` (required, array[ObligationTotals], fixed-type)

    + Body

            {
                "award_obligations": 99999999.99,
                "results": [
                    "contracts": [
                        {
                            "type": "contracts",
                            "obligated_amount": 9999999.99
                        },
                        {
                            "type": "idvs",
                            "obligated_amount": 9999999.99
                        }
                    ],
                    "assistance": [
                        {
                            "type": "direct_payments",
                            "obligated_amount": 9999999.99
                        },
                        {
                            "type": "grants",
                            "obligated_amount": 9999999.99
                        }
                    ]
                ]
            }

# Data Structures

## ObligationTotals (object)
+ `contracts` (required, array[Obligation], fixed-type)
+ `assistance` (required, array[Obligation], fixed-type)
## Obligation (object)
+ `type` (required, enum[string])
    + Members
        + `contracts`
        + `idvs`
        + `grants`
        + `loans`
        + `direct_payments`
        + `other`
+ `obligated_amount` (required, number)