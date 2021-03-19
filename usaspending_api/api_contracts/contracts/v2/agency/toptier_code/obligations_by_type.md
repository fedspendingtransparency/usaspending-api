FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/agency/{toptier_code}/obligations_by_type/{?fiscal_year}]

This endpoint is used to power USAspending.gov's agency profile pages.

## GET

This endpoint returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other) within the requested fiscal year (or current FY).

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year`: 2017 (optional, number)
        The fiscal year for which you are querying data. Defaults to the current fiscal year if not provided.
        
+ Response 200 (application/json)
    + Attributes (object)
        + `total_aggregated_amount` (required, number)
        + `results` (required, array[ObligationSubtotals], fixed-type)
            Sorted by aggregated_amount, descending. Categories with $0 of obligations within the FY are not included.
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "total_aggregated_amount": 39999999.96,
                "results": [
                    {
                        "category": "contracts",
                        "aggregated_amount": 9999999.99
                    },
                    {
                        "category": "idvs",
                        "aggregated_amount": 9999999.99
                    },
                    {
                        "category": "direct_payments",
                        "aggregated_amount": 9999999.99
                    },
                    {
                        "category": "grants",
                        "aggregated_amount": 9999999.99
                    }
                ],
                "messages": []
            }

# Data Structures

## ObligationSubtotals (object)
+ `category` (required, enum[string])
    + Members
        + `contracts`
        + `idvs`
        + `grants`
        + `loans`
        + `direct_payments`
        + `other`
+ `aggregated_amount` (required, number)
